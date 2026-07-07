//
//  PostgreSQLAdaptor.swift
//  ZeeQL
//
//  Created by Helge Hess on 03/03/17.
//  Copyright © 2017-2026 ZeeZide GmbH. All rights reserved.
//

#if os(Windows)
  import WinSDK
#elseif os(Linux)
  import Glibc
#else
  import Darwin
#endif
import Foundation
import ZeeQL
import CLibPQ

fileprivate let BinaryFlag : Int32 = 1

public enum PostgreSQLAdaptorChannelError: Swift.Error {

  case execError  (reason: String, sql: String)
  case badResponse(reason: String, sql: String)
  case fatalError (reason: String, sql: String)
  case unsupportedResultType(String)

  case connectionClosed
}

open class PostgreSQLAdaptorChannel : AdaptorChannel, SmartDescription {

  static let isDebugDefaultOn =
               UserDefaults.standard.bool(forKey: "PGDebugEnabled")

  public let expressionFactory : SQLExpressionFactory
  public var handle : OpaquePointer?
  final  let logSQL : Bool
  
  init(adaptor: Adaptor, handle: OpaquePointer) {
    self.expressionFactory = adaptor.expressionFactory
    self.handle = handle
    self.logSQL = Self.isDebugDefaultOn
  }
  
  deinit {
    if let handle = handle { PQfinish(handle) }
  }
  
  func close() {
    guard let handle = handle else { return }
    PQfinish(handle)
    self.handle = nil
  }

  /// The backend process ID of this channel's PG connection, or 0 if the
  /// channel has been closed (PQbackendPID).
  @inlinable
  public var backendProcessID : Int32 {
    guard let handle = handle else { return 0 }
    return PQbackendPID(handle)
  }


  
  // MARK: - Raw Queries
  
  /**
   * Iterate over the raw result set and produce `AdaptorRecord`s.
   */
  func fetchRows(_ res      : OpaquePointer,
                 _ optAttrs : [ Attribute ]? = nil,
                 yield      : ( AdaptorRecord ) throws -> Void) throws
  {
    // The libpq function fetches everything into memory I think. Need to
    // drop libpq eventually ;-)
    
    let binary   = PQbinaryTuples(res) != 0
    let count    = Int(PQntuples(res))
    let colCount = Int(PQnfields(res))
    
    var schema : AdaptorRecordSchema
      // assumes uniform results, which should be so
    
    if let attrs = optAttrs {
      schema = AdaptorRecordSchemaWithAttributes(attrs)
    }
    else {
      // TBD: Do we want to build attributes? Probably not, too expensive for
      //      simple stuff.
      var names = [ String ]()
      names.reserveCapacity(colCount)
      
      for colIdx in 0..<colCount {
        #if true
          if let name = PQfname(res, Int32(colIdx)) {
            names.append(String(cString: name))
          }
          else {
            names.append("col[\(colIdx)]")
          }
        #else // old, 'fillup' mode where some are provided by optAttrs
          if let attrs = optAttrs, colIdx < attrs.count,
             let col = attrs[colIdx].columnName
          {
            names.append(col)
          }
          else if let name = PQfname(res, Int32(colIdx)) {
            names.append(String(cString: name))
          }
          else {
            names.append("col[\(colIdx)]")
          }
        #endif
      }
      schema = AdaptorRecordSchemaWithNames(names)
    }
    
    // TBD: This is a little inefficient, would be better to let the caller
    //      know the number of records retrieved, so that it can reserve the
    //      capacity.
    //      However, the _real_ fix is to perform incremental fetches.
    for i in 0..<count {
      var values = [ Any? ]()
      values.reserveCapacity(colCount)
      
      for colIdx in 0..<colCount {
        let attr : Attribute?
        if let attrs = optAttrs, colIdx < attrs.count {
          attr = attrs[colIdx]
        }
        else {
          attr = nil
        }
        
        let row = Int32(i)
        let col = Int32(colIdx)
        if PQgetisnull(res, row, col) != 0 {
          // TODO: consider value type of attr
          // TBD: a little weird :-)
          // TBD: why can't we use Any? = nil?
          values.append(Optional<String>.none)
          continue
        }
        guard let pgValue = PQgetvalue(res, row, col) else {
          values.append(Optional<String>.none)
          continue
        }
        
        let type = PQftype(res, col)
        let len  = PQgetlength(res, row, col)
        
        let bptr = UnsafeBufferPointer(start: pgValue, count: Int(len))
        let value = binary
          ? valueForBinaryPGValue(type: type, value: bptr, attribute: attr)
          : valueForTextPGValue  (type: type, value: bptr, attribute: attr)
        
        values.append(value)
      }
      
      let record = AdaptorRecord(schema: schema, values: values)
      try yield(record)
    }
  }
  
  
  /**
   * Execute `sql` against and yield results.
   *
   * When `bindings` is `nil`/empty and `allowMultiStatement` is `true`,
   * the call is routed through `PQexec`, which accepts multiple
   * `;`-separated statements but always returns results in libpq's
   * text format.
   * All other calls go through `PQexecParams`, which can only do single
   * statement but binary results.
   *
   * - Parameters:
   *   - sql:                 The SQL statement(s).
   *   - optAttrs:            Optional ``Attribute``'s describing the
   *                          expected result columns. When provided,
   *                          they form the schema of the ``AdaptorRecord``'s
   *                          passed to `yield`, other names come from result
   *                          set.
   *   - bindings:            Parameter bindings or nil, if there are none.
   *   - allowMultiStatement: Set to `true` to permit multiple statement SQL.
   *                          Only safe when the caller does not need to consume
   *                          rows. (default: false)
   *   - yield:               Row result closure.
   * - Returns: The number of rows affected, or nil for empty input/select.
   * - Throws:  ``PostgreSQLAdaptorChannelError`` on connection or
   *            execution failures, or any error raised by `yield`.
   */
  private func _runSQL(sql: String, optAttrs : [ Attribute ]?,
                       bindings: [ SQLExpression.BindVariable ]?,
                       allowMultiStatement: Bool = false,
                       yield: ( AdaptorRecord ) throws -> Void) throws
               -> Int?
  {
    guard let handle = handle else {
      throw PostgreSQLAdaptorChannelError.connectionClosed
    }

    let defaultReason = "Could not performSQL"
    if logSQL { print("SQL: \(sql)") }
    
    
    // bindings

    let bindingCount    = bindings?.count ?? 0
    var bindingTypes    = [ Oid   ]()
    var bindingLengths  = [ Int32 ]()
    var bindingIsBinary = [ Int32 ]()
    
    // TODO: allocations in here are wasteful and need to be improved (e.g. a
    //       common alloc block?)
    var bindingValues   = [ UnsafePointer<Int8>? ]()
    defer {
      for value in bindingValues {
        guard let value = value else { continue }
        free(UnsafeMutableRawPointer(mutating: value))
      }
    }
    
    var idx = 0
    if let bindings = bindings {
        // TODO: avoid allocating the arrays, but we have other overheads here
      bindingTypes   .reserveCapacity(bindingCount)
      bindingLengths .reserveCapacity(bindingCount)
      bindingIsBinary.reserveCapacity(bindingCount)
      bindingValues  .reserveCapacity(bindingCount)
      
      for bind in bindings {
        // if logSQL { print("  BIND[\(idx)]: \(bind)") }
        
        if let attr = bind.attribute {
          if logSQL { print("  BIND[\(idx)]: \(attr.name)") }
          
          // TODO: ask attribute for OID
        }
        
        // FIXME(hh 2024-11-25): Unnested all this stuff.
        // TODO: Add a protocol to do this?
        func bindAnyValue(_ value: Any?) throws -> Bind {
          guard let value = value else {
            if logSQL { print("      [\(idx)]> bind NULL") }
            return Bind(type: 0 /*Hmmm*/, length: 0, rawValue: nil)
          }
          
          if let value = value as? PGBindableValue {
            return try value.bind(index: idx, log: logSQL)
          }
          
          if let variable = value as? QualifierVariable {
            struct UnresolvedQualifier: Swift.Error {
              let key : String
            }
            throw UnresolvedQualifier(key: variable.key)
          }
          
          if logSQL { print("      [\(idx)]> bind other \(value)") }
          assertionFailure("Unexpected value, please add explicit type")
          let rawValue = UnsafePointer(strdup(String(describing: value)))
          return Bind(type: OIDs.VARCHAR,
                      length: rawValue.flatMap { Int32(strlen($0)) } ?? 0,
                      rawValue: rawValue)
        }
        
        let bindInfo = try bindAnyValue(bind.value)
        
        bindingTypes   .append(bindInfo.type)
        bindingLengths .append(bindInfo.length)
        bindingIsBinary.append(bindInfo.isBinary)
        bindingValues  .append(bindInfo.rawValue)
      }
      
      idx += 1
    }
    
    // types, values, length, binaryOrNot

    // `PQexec` supports multiple `;`-separated statements but always
    // returns results in TEXT format.
    // Only `performSQL` opts into multi-statement support (discards rows).
    guard let result = (allowMultiStatement && bindingCount == 0)
      ? PQexec(handle, sql)
      : PQexecParams(handle, sql, Int32(bindingCount),
                     bindingTypes, bindingValues, bindingLengths,
                     bindingIsBinary, BinaryFlag) else
    {
      throw PostgreSQLAdaptorChannelError
        .execError(reason: lastError ?? defaultReason, sql: sql)
    }
    defer { PQclear(result) }
    
    let status = PQresultStatus(result)
    switch status {
      case PGRES_TUPLES_OK:
        try fetchRows(result, optAttrs, yield: yield)
      
      case PGRES_EMPTY_QUERY: return nil // string was empty :-)
      case PGRES_COMMAND_OK:  break      // no data
      
      case PGRES_NONFATAL_ERROR:
        throw PostgreSQLAdaptorChannelError
                .execError(reason: lastError ?? defaultReason, sql: sql)

      case PGRES_FATAL_ERROR:
        throw PostgreSQLAdaptorChannelError
                .fatalError(reason: lastError ?? defaultReason, sql: sql)

      case PGRES_BAD_RESPONSE:
        // TBD: close connection?
        throw PostgreSQLAdaptorChannelError
                .badResponse(reason: lastError ?? defaultReason, sql: sql)

      // TODO: support COPY
      case PGRES_COPY_IN:
        throw PostgreSQLAdaptorChannelError.unsupportedResultType("COPY_IN")
      case PGRES_COPY_OUT:
        throw PostgreSQLAdaptorChannelError.unsupportedResultType("COPY_OUT")
      case PGRES_COPY_BOTH:
        throw PostgreSQLAdaptorChannelError.unsupportedResultType("COPY_BOTH")
      default:
        throw PostgreSQLAdaptorChannelError.unsupportedResultType("\(status)")
    }
    
    guard let cstr = PQcmdTuples(result) else { return nil }
    guard cstr.pointee != 0              else { return nil } // empty string
    return atol(cstr)
  }
  
  public func querySQL(_ sql: String, _ optAttrs : [ Attribute ]?,
                         cb: ( AdaptorRecord ) throws -> Void) throws
  {
    _ = try _runSQL(sql: sql, optAttrs: optAttrs, bindings: nil, yield: cb)
  }
  
  @discardableResult
  public func performSQL(_ sql: String) throws -> Int {
    // Hm, funny. If we make 'cb' optional, it becomes escaping. So avoid that.
    return try _runSQL(sql: sql, optAttrs: nil, bindings: nil,
                       allowMultiStatement: true) { rec in } ?? 0
  }
  
  
  // MARK: - Values
  
  func valueForBinaryPGValue(type: Oid, value: UnsafeBufferPointer<Int8>,
                             attribute: Attribute?) -> Any?
  {
    // TODO: consider attribute! (e.g. for date, valueType in attr, if set)
    
    // TODO: decode actual types :-)
    // TODO: do not crash on force unwrap
    
    switch type {
      case OIDs.INT2:
        guard let addr = value.baseAddress else { return nil }
        return Int16(bigEndian: cast(addr))
      case OIDs.INT4:
        guard let addr = value.baseAddress else { return nil }
        return Int32(bigEndian: cast(addr))
      case OIDs.INT8:
        guard let addr = value.baseAddress else { return nil }
        return Int64(bigEndian: cast(addr))
      
      // `Float` has no `bigEndian`, so we swap the raw bit pattern (an
      // integer of the same width) and reinterpret. This mirrors the bind
      // side (`BinaryFloatingPoint.bind`), which writes
      // `value.bitPattern.bigEndian`.
      case OIDs.FLOAT4:
        guard let addr = value.baseAddress else { return nil }
        return Float32(bitPattern: UInt32(bigEndian: cast(addr)))
      case OIDs.FLOAT8:
        guard let addr = value.baseAddress else { return nil }
        return Float64(bitPattern: UInt64(bigEndian: cast(addr)))

      case OIDs.BOOL:    return (value.baseAddress!.pointee != 0)
      
      case OIDs.VARCHAR, OIDs.TEXT, OIDs.CHAR:
        guard let addr = value.baseAddress else { return nil }
        return String(cString: addr)
      
      case OIDs.NAME: // e.g. SELECT datname FROM pg_database
        guard let addr = value.baseAddress else { return nil }
        return String(cString: addr)
      
      case OIDs.TIMESTAMPTZ: // 1184
        // TODO: I think it is better to fix this during the query, that is,
        // to a SELECT unix_time(startDate) like thing.
        // hm. How to parse this? We used to have the format in the attribute?
        // http://www.linuxtopia.org/online_books/database_guides/Practical_PostgreSQL_database/PostgreSQL_x2632_005.htm
        // 2024-11-08(hh): this seems to be 8 bytes aka UInt64 and the above
        //                 for String representations
        // https://postgrespro.com/list/thread-id/1482672
        // https://materialize.com/docs/sql/types/timestamp/
        // - Min value    4713 BC
        // - Max value  294276 AD
        // - Max resolution  1 microsecond
        if value.count == 8 {
          // 1_000_000
          let msecs = Double(Int64(bigEndian: cast(value.baseAddress!)))
          let date = Date(timeInterval: TimeInterval(msecs) / 1000000.0,
                          since: Date.pgReferenceDate)
          return date
        }
        guard let addr = value.baseAddress else { return nil }
        return String(cString: addr)
      case OIDs.TIMESTAMP:
        // Same 8-byte microseconds-since-2000 layout as TIMESTAMPTZ, just
        // without an attached zone. We interpret it as UTC and return a
        // `Date` (was: mis-read as a `String` in binary mode => garbage).
        if value.count == 8 {
          let msecs = Double(Int64(bigEndian: cast(value.baseAddress!)))
          return Date(timeInterval: TimeInterval(msecs) / 1000000.0,
                      since: Date.pgReferenceDate)
        }
        guard let addr = value.baseAddress else { return nil }
        return String(cString: addr)

      case OIDs.OID:
        // https://www.postgresql.org/docs/9.5/static/datatype-oid.html
        guard let addr = value.baseAddress else { return nil }
        return UInt32(bigEndian: cast(addr))
      
        
      case OIDs.ARRAY_OF_TEXT:
        return decodeTextArray(from: UnsafeRawBufferPointer(value))

      case OIDs.JSONB:
        // Binary `jsonb` is a 1-byte version header (currently `0x01`)
        // followed by the UTF-8 JSON text. We strip the header and return
        // the JSON as `Data`, which is what `JSONDecoder` /
        // `JSONSerialization` consume directly.
        guard let addr = value.baseAddress, value.count >= 1 else { return nil }
        return Data(bytes: addr + 1, count: value.count - 1)
      case OIDs.JSON:
        // Binary `json` is just the UTF-8 text, no version header.
        guard let addr = value.baseAddress else { return nil }
        return Data(bytes: addr, count: value.count)

      case OIDs.UUID:
        // 16 raw bytes in network order, which is exactly `uuid_t`.
        guard let addr = value.baseAddress, value.count == 16 else { return nil }
        var bytes = uuid_t(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        withUnsafeMutableBytes(of: &bytes) {
          $0.copyBytes(from: UnsafeRawBufferPointer(start: addr, count: 16))
        }
        return UUID(uuid: bytes)

      case OIDs.NUMERIC:
        return decodeNumeric(from: UnsafeRawBufferPointer(value))

      default:
        print("Unexpected OID: \(type): \(String(cString:value.baseAddress!))")
        return Data(buffer: value)
    }
  }
  
  func valueForTextPGValue(type: Oid, value: UnsafeBufferPointer<Int8>,
                           attribute: Attribute?) -> Any?
  {
    // TODO: consider attribute! (e.g. for date, valueType in attr, if set)
    // - What we want is that the class is grabbed from the attribute, and
    //   if that is a PostgreSQLDecodable, we pass it the type and everything.
    
    // TODO: decode actual types :-)
    
    switch type {
      case OIDs.INT2:
        guard let base = value.baseAddress else { return Int16(0) }
        return Int16(atol(base))
      
      case OIDs.INT4:
        guard let base = value.baseAddress else { return Int32(0) }
        return Int32(atol(base))
      
      case OIDs.FLOAT4:
        guard let base = value.baseAddress else { return Float32(0) }
        return Float32(atof(base))
      
      case OIDs.FLOAT8:
        guard let base = value.baseAddress else { return Float64(0) }
        return Float64(atof(base))
      
      case OIDs.VARCHAR:
        guard let base = value.baseAddress else { return Optional<String>.none }
        return String(cString: base)
      
      case OIDs.TIMESTAMPTZ:
        // TODO: I think it is better to fix this during the query, that is,
        // to a SELECT unix_time(startDate) like thing.
        // hm. How to parse this? We used to have the format in the attribute?
        // http://www.linuxtopia.org/online_books/database_guides/Practical_PostgreSQL_database/PostgreSQL_x2632_005.htm
        guard let base = value.baseAddress else { return Optional<String>.none }
        return String(cString: base)
      
      default:
        print("OID: \(type): \(String(cString:value.baseAddress!))")
        guard let base = value.baseAddress else { return Optional<String>.none }
        return String(cString: base)
    }
  }
 
  
  // MARK: - Model Queries
  
  public func evaluateQueryExpression(_ sqlexpr  : SQLExpression,
                                      _ optAttrs : [ Attribute ]?,
                                      result: ( AdaptorRecord ) throws -> Void)
                throws
  {
    _ = try _runSQL(sql: sqlexpr.statement, optAttrs: optAttrs,
                    bindings: sqlexpr.bindVariables, yield: result)
  }

  public func evaluateUpdateExpression(_ sqlexpr: SQLExpression) throws -> Int {
    return try _runSQL(sql: sqlexpr.statement, optAttrs: nil,
                       bindings: sqlexpr.bindVariables) { rec in } ?? 0
  }
  
  
  // MARK: - Transactions
  
  public var isTransactionInProgress : Bool = false
  
  @inlinable
  public func begin() throws {
    guard !isTransactionInProgress
     else { throw AdaptorChannelError.transactionInProgress }
    
    try performSQL("BEGIN TRANSACTION;")
    isTransactionInProgress = true
  }
  @inlinable
  public func commit() throws {
    isTransactionInProgress = false
    try performSQL("COMMIT TRANSACTION;")
  }
  @inlinable
  public func rollback() throws {
    isTransactionInProgress = false
    try performSQL("ROLLBACK TRANSACTION;")
  }
  
  
  // MARK: - Errors
  
  var lastError : String? {
    guard let cstr = PQerrorMessage(handle) else { return nil }
    return String(cString: cstr)
  }

  
  // MARK: - Description
  
  public func appendToDescription(_ ms: inout String) {
    if let handle = handle {
      ms += " \(handle)"
    }
    else {
      ms += " finished"
    }
  }
  

  // MARK: - reflection
  
  @inlinable
  public func describeSequenceNames() throws -> [ String ] {
    return try PostgreSQLModelFetch(channel: self).describeSequenceNames()
  }
  
  @inlinable
  public func describeDatabaseNames() throws -> [ String ] {
    return try PostgreSQLModelFetch(channel: self).describeDatabaseNames()
  }
  @inlinable
  public func describeTableNames() throws -> [ String ] {
    return try PostgreSQLModelFetch(channel: self).describeTableNames()
  }

  @inlinable
  public func describeEntityWithTableName(_ table: String) throws -> Entity? {
    return try PostgreSQLModelFetch(channel: self)
                 .describeEntityWithTableName(table)
  }
  @inlinable
  public func describeEntitiesWithTableNames(_ tableNames: [ String ])
                throws -> [ Entity ]
  {
    return try PostgreSQLModelFetch(channel: self)
                 .describeEntitiesWithTableNames(tableNames)
  }

  
  // MARK: - Insert w/ auto-increment support
  
  @inlinable
  open func insertRow(_ row: AdaptorRow, _ entity: Entity, refetchAll: Bool)
              throws -> AdaptorRow
  {
    let attributes : [ Attribute ]? = {
      if refetchAll { return entity.attributes }
      
      // TBD: refetch-all if no pkeys are assigned
      guard let pkeys = entity.primaryKeyAttributeNames, !pkeys.isEmpty
       else { return entity.attributes }
      
      return entity.attributesWithNames(pkeys)
    }()
    
    let expr = PostgreSQLExpression(entity: entity)
    expr.prepareInsertReturningExpressionWithRow(row, attributes: attributes)
    
    var rec : AdaptorRecord? = nil
    try evaluateQueryExpression(expr, attributes) { record in
      guard rec == nil else { // multiple matched!
        throw AdaptorError.failedToRefetchInsertedRow(
                             entity: entity, row: row)
      }
      rec = record
    }
    guard let rrec = rec else { // no record returned?
      throw AdaptorError.failedToRefetchInsertedRow(entity: entity, row: row)
    }
    
    return rrec.asAdaptorRow
  }
}

fileprivate func cast<T>(_ value: UnsafePointer<Int8>) -> T {
  return value.withMemoryRebound(to: T.self, capacity: 1) { typedPtr in
    typedPtr.pointee
  }
}

fileprivate func tdup<T>(_ value: T) -> UnsafeBufferPointer<Int8> {
  let len = MemoryLayout<T>.size
  let raw = OpaquePointer(malloc(len)!)
  let ptr = UnsafeMutablePointer<T>(raw)
  ptr.pointee = value
  return UnsafeBufferPointer(start: UnsafePointer(raw), count: len)
}

#if canImport(Foundation)
fileprivate extension Date {
  
  // 2000-01-01
  static let pgReferenceDate = Date(timeIntervalSince1970: 946684800)
}
#endif


// MARK: - Binding

fileprivate struct Bind {
  // So this always has the value *malloc*'ed in rawValue, which is not
  // particularily great :-)
  
  var type     : Oid   = 0
  var length   : Int32 = 0
  var isBinary : Int32 = BinaryFlag
  var rawValue : UnsafePointer<Int8>? = nil
}

fileprivate protocol PGBindableValue {
  
  func bind(index: Int, log: Bool) throws -> Bind
}

extension Optional: PGBindableValue where Wrapped: PGBindableValue {
  
  fileprivate func bind(index idx: Int, log: Bool) throws -> Bind {
    switch self {
      case .some(let value): return try value.bind(index: idx, log: log)
      case .none:
        if log { print("      [\(idx)]> bind NULL") }
        return Bind(type: 0 /*Hmmm*/, length: 0, rawValue: nil)
    }
  }
}

extension String: PGBindableValue {
  
  fileprivate func bind(index idx: Int, log: Bool) throws -> Bind {
    if log { print("      [\(idx)]> bind string \"\(self)\"") }
    // TODO: include 0 in length?
    let rawValue = UnsafePointer(strdup(self))
    return Bind(type: OIDs.VARCHAR,
                length: rawValue.flatMap { Int32(strlen($0)) } ?? 0,
                rawValue: rawValue)
  }
}
extension Bool: PGBindableValue {
  
  fileprivate func bind(index idx: Int, log: Bool) throws -> Bind {
    if log { print("      [\(idx)]> bind int \(self)") }
    let value = UInt8(self ? 0x1 : 0x0)
    let bp    = tdup(value)
    return Bind(type: OIDs.BOOL, length: Int32(1), rawValue: bp.baseAddress!)
  }
}
extension BinaryInteger {
  
  fileprivate func bind(index idx: Int, log: Bool) throws -> Bind {
    if log { print("      [\(idx)]> bind int \(self)") }
    let value = Int(self) // Hmm
    let bp    = tdup(value.bigEndian)
    return Bind(type: MemoryLayout<Int>.size == 8
                ? OIDs.INT8 : OIDs.INT4,
                length: Int32(bp.count), rawValue: bp.baseAddress!)
  }
}
extension Int   : PGBindableValue {}
extension UInt  : PGBindableValue {}
extension Int32 : PGBindableValue {}
extension Int64 : PGBindableValue {}

extension BinaryFloatingPoint {

  fileprivate func bind(index idx: Int, log: Bool) throws -> Bind {
    if log { print("      [\(idx)]> bind float \(self)") }
    let value = Double(self)
    let bp    = tdup(value.bitPattern.bigEndian)
    return Bind(type: OIDs.FLOAT8,
                length: Int32(bp.count),
                rawValue: bp.baseAddress!)
  }
}
extension Double : PGBindableValue {}
extension Float  : PGBindableValue {}

#if canImport(Foundation)
extension Date: PGBindableValue {
  
  fileprivate func bind(index idx: Int, log: Bool) throws -> Bind {
    let diff  = self.timeIntervalSince(Date.pgReferenceDate)
    let msecs = Int64(diff * 1000000.0) // seconds to milliseconds
    let bp    = tdup(msecs.bigEndian)
    return Bind(type: OIDs.TIMESTAMPTZ, // 1184
                length: 8,
                rawValue: bp.baseAddress!)
  }
}

extension UUID: PGBindableValue {
  fileprivate func bind(index idx: Int, log: Bool) throws -> Bind {
    return try uuidString.bind(index: idx, log: log)
  }
}
#endif

extension KeyGlobalID: PGBindableValue {
  
  fileprivate func bind(index idx: Int, log: Bool) throws -> Bind {
    assert(keyCount == 1)
    switch value {
      case .singleNil:
        return try Optional<String>.none.bind(index: idx, log: log)
      case .int   (let value) : return try value.bind(index: idx, log: log)
      case .string(let value) : return try value.bind(index: idx, log: log)
      case .uuid  (let value) : return try value.bind(index: idx, log: log)
      case .values(let values):
        if values.count > 1 {
          throw PostgreSQLAdaptorChannelError
            .execError(reason: "Invalid multi-gid bind", sql: "")
        }
        assert(values.first is PGBindableValue)
        if let value = values.first as? PGBindableValue {
          return try value.bind(index: idx, log: log)
        }
        else { return try Optional<String>.none.bind(index: idx, log: log) }
    }
  }
}

fileprivate func decodeTextArray(from buffer: UnsafeRawBufferPointer) -> Any? {
  // arrays, header:
  // - dimensions:  Int32      // e.g. 1
  // - flags:       UInt32     // 0=no nulls, 1=nulls
  // - elementType: OID/UInt32 // e.g. TEXT(25) for TEXT[](1009)
  // then for each dimension:
  // - count:       Int32
  // - lower bound: Int32      // usually 1
  // then for each elements:
  // - size:        Int32
  // - value:       ^^^        // if no tnull
  // value in TEXT[] is UTF-8 string, not null terminated
  guard var cursor = buffer.baseAddress else { return nil }
  
  func readInt32() -> Int32 {
    defer { cursor += MemoryLayout<Int32>.size }
    return cursor.assumingMemoryBound(to: Int32.self).pointee.bigEndian
  }
  func readString(_ len: Int) -> String? {
    if len < 0 { return nil }
    let slice = UnsafeRawBufferPointer(start: cursor, count: len)
    cursor += Int(len)
    return String(decoding: slice, as: UTF8.self)
  }

  // header
  let ndim    = readInt32()
  guard ndim != 0 else { return [] } // empty arrays have dim 0
  
  let hasNull = readInt32() != 0 // flags, currently on 0 or 1?!
  let elemOID = readInt32()
  assert(elemOID == OIDs.TEXT)
  guard ndim == 1 else {
    assertionFailure("Only 1D arrays supported")
    return nil
  }
  
  
  do { // one dimension
    let count      = readInt32()
    assert(count >= 0 && count <= 1_000_000_000)
    if count <= 0 { return Array<String>() }
    
    let lowerBound = readInt32()
    assert(lowerBound == 1, "unexpected lower bound")
    
    if hasNull {
      var result = [ String? ](); result.reserveCapacity(Int(count))
      
      for _ in 0..<count {
        let len = readInt32()
        result.append(readString(Int(len)))
      }
      return result
    }
    else {
      var result = [ String ](); result.reserveCapacity(Int(count))
      
      for _ in 0..<count {
        let len = readInt32()
        if let s = readString(Int(len)) {
          result.append(s)
        }
        else {
          assertionFailure("Array set to non-null, but contains nulls?")
          result.append("")
        }
      }
      return result
    }
  }
}

/**
 * Decode a PostgreSQL `numeric` from its binary wire format into a Foundation
 * `Decimal`. `Decimal` is the right target: like `numeric` it is base-10 and
 * exact, unlike `Double`.
 *
 * Wire format (`numeric_send`):
 * - Int16  ndigits   number of base-10000 digit groups
 * - Int16  weight    base-10000 weight of the first group
 * - UInt16 sign      0x0000 pos, 0x4000 neg, 0xC000 NaN, 0xD000/0xF000 Inf
 * - Int16  dscale    display scale (digits after the point; value-neutral)
 * - Int16  digits[ndigits]  each in 0...9999
 *
 * Returns `nil` for `NaN`/`±Infinity` (not representable as a finite
 * `Decimal`) rather than fabricating a wrong number.
 */
fileprivate func decodeNumeric(from buffer: UnsafeRawBufferPointer) -> Any? {
  // PostgreSQL `numeric` sign words (see `numeric.h`).
  let numericSignNeg  : UInt16 = 0x4000
  let numericSignNaN  : UInt16 = 0xC000
  let numericSignPInf : UInt16 = 0xD000
  let numericSignNInf : UInt16 = 0xF000

  guard let base = buffer.baseAddress, buffer.count >= 8 else { return nil }
  var offset = 0

  func readInt16() -> Int16 {
    defer { offset += MemoryLayout<Int16>.size }
    return base.loadUnaligned(fromByteOffset: offset, as: Int16.self).bigEndian
  }

  let ndigits = Int(readInt16())
  let weight  = Int(readInt16())
  let sign    = UInt16(bitPattern: readInt16())
  _ = readInt16() // dscale, only affects display, not the value

  switch sign {
    case numericSignNaN, numericSignPInf, numericSignNInf: return nil
    default: break
  }
  guard ndigits >= 0,
        buffer.count >= (8 + ndigits * MemoryLayout<Int16>.size) else
  {
    return nil
  }

  var result = Decimal(0)
  for i in 0..<ndigits {
    let group = Int(readInt16()) // 0...9999
    // group * 10000^(weight - i) == group * 10^(4 * (weight - i))
    result += Decimal(sign: .plus, exponent: 4 * (weight - i),
                      significand: Decimal(group))
  }
  if sign == numericSignNeg { result.negate() }
  return result
}
