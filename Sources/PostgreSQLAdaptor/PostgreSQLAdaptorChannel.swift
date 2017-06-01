//
//  PostgreSQLAdaptor.swift
//  ZeeQL
//
//  Created by Helge Hess on 03/03/17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import struct Foundation.Data
import ZeeQL
import CLibPQ

fileprivate let BinaryFlag : Int32 = 1

open class PostgreSQLAdaptorChannel : AdaptorChannel, SmartDescription {

  public enum Error : Swift.Error {
    case ExecError  (reason: String, sql: String)
    case BadResponse(reason: String, sql: String)
    case FatalError (reason: String, sql: String)
    case UnsupportedResultType(String)
    
    case Generic
    case NotImplemented
    case ConnectionClosed
  }

  public let expressionFactory : SQLExpressionFactory
  public var handle : OpaquePointer?
  final  let logSQL  = true
  
  init(adaptor: Adaptor, handle: OpaquePointer) {
    self.expressionFactory = adaptor.expressionFactory
    self.handle = handle
  }
  
  deinit {
    if let handle = handle { PQfinish(handle) }
  }
  
  func close() {
    guard let handle = handle else { return }
    PQfinish(handle)
    self.handle = nil
  }
  
  
  // MARK: - Raw Queries
  
  /**
   * Iterate over the raw result set and produce `AdaptorRecord`s.
   */
  func fetchRows(_ res      : OpaquePointer,
                 _ optAttrs : [ Attribute ]? = nil,
                 cb         : ( AdaptorRecord ) throws -> Void) throws
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
      try cb(record)
    }
  }
  
  
  private func _runSQL(sql: String, optAttrs : [ Attribute ]?,
                       bindings: [ SQLExpression.BindVariable ]?,
                       cb: ( AdaptorRecord ) throws -> Void) throws
               -> Int?
  {
    guard let handle = handle else { throw Error.ConnectionClosed }
    
    let defaultReason = "Could not performSQL"
    if logSQL { print("SQL: \(sql)") }
    
    
    // bindings
    // TODO: avoid creating the arrays, but we have other overheads here
    
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
      bindingTypes   .reserveCapacity(bindingCount)
      bindingLengths .reserveCapacity(bindingCount)
      bindingIsBinary.reserveCapacity(bindingCount)
      bindingValues  .reserveCapacity(bindingCount)
      
      for bind in bindings {
        // if logSQL { print("  BIND[\(idx)]: \(bind)") }
        
        let type     : Oid
        let length   : Int32
        let isBinary : Int32 = BinaryFlag
        let rawValue : UnsafePointer<Int8>?

        if let attr = bind.attribute {
          if logSQL { print("  BIND[\(idx)]: \(attr.name)") }
          
          // TODO: ask attribute for OID
        }
        
        // TODO: Add a protocol to do this?
        if let value = bind.value {
          if let value = value as? String {
            if logSQL { print("      [\(idx)]> bind string \"\(value)\"") }
            type     = OIDs.VARCHAR
            rawValue = UnsafePointer(strdup(value))
            length   = rawValue != nil ? Int32(strlen(rawValue!)) : 0
              // TODO: include 0 in length?
          }
          else if let value = value as? SingleIntKeyGlobalID { // hacky
            if logSQL { print("      [\(idx)]> bind key \(value)") }
            type     = MemoryLayout<Int>.size == 8 ? OIDs.INT8 : OIDs.INT4
            let bp   = tdup(value.value.bigEndian)
            rawValue = bp.baseAddress!
            length   = Int32(bp.count)
          }
          else if let value = value as? Int { // TODO: Other Integers
            if logSQL { print("      [\(idx)]> bind int \(value)") }
            type     = MemoryLayout<Int>.size == 8 ? OIDs.INT8 : OIDs.INT4
            let bp   = tdup(value.bigEndian)
            rawValue = bp.baseAddress!
            length   = Int32(bp.count)
          }
          else { // TODO
            if logSQL { print("      [\(idx)]> bind other \(value)") }
            type = OIDs.VARCHAR
            rawValue = UnsafePointer(strdup("\(value)"))
            length   = rawValue != nil ? Int32(strlen(rawValue)) : 0
              // TODO: include 0 in length?
          }
        }
        else {
          if logSQL { print("      [\(idx)]> bind NULL") }
          // TODO: set value to NULL
          type     = 0 // Hm
          length   = 0
          rawValue = nil
        }
        
        bindingTypes   .append(type)
        bindingLengths .append(length)
        bindingIsBinary.append(isBinary)
        bindingValues  .append(rawValue)
      }
      
      idx += 1
    }
    
    // types, values, length, binaryOrNot
    
    // PGresult
    guard let result = PQexecParams(handle, sql,
                                    Int32(bindingCount),
                                    bindingTypes,
                                    bindingValues,
                                    bindingLengths,
                                    bindingIsBinary, BinaryFlag)
     else {
      throw Error.ExecError(reason: lastError ?? defaultReason,
                            sql: sql)
    }
    defer { PQclear(result) }
    
    let status = PQresultStatus(result)
    switch status {
      case PGRES_TUPLES_OK:
        try fetchRows(result, optAttrs, cb: cb)
      
      case PGRES_EMPTY_QUERY: return nil // string was empty :-)
      case PGRES_COMMAND_OK:  break      // no data
      
      case PGRES_NONFATAL_ERROR:
        throw Error.ExecError(reason: lastError ?? defaultReason, sql: sql)

      case PGRES_FATAL_ERROR:
        throw Error.FatalError(reason: lastError ?? defaultReason, sql: sql)
      
      case PGRES_BAD_RESPONSE:
        // TBD: close connection?
        throw Error.BadResponse(reason: lastError ?? defaultReason, sql: sql)
      
      // TODO: support COPY
      case PGRES_COPY_IN:   throw Error.UnsupportedResultType("COPY_IN")
      case PGRES_COPY_OUT:  throw Error.UnsupportedResultType("COPY_OUT")
      case PGRES_COPY_BOTH: throw Error.UnsupportedResultType("COPY_BOTH")
      default:              throw Error.UnsupportedResultType("\(status)")
    }
    
    guard let cstr = PQcmdTuples(result) else { return nil }
    guard cstr.pointee != 0              else { return nil } // empty string
    return atol(cstr)
  }
  
  public func querySQL(_ sql: String, _ optAttrs : [ Attribute ]?,
                         cb: ( AdaptorRecord ) throws -> Void) throws
  {
    _ = try _runSQL(sql: sql, optAttrs: optAttrs, bindings: nil, cb: cb)
  }
  
  @discardableResult
  public func performSQL(_ sql: String) throws -> Int {
    // Hm, funny. If we make 'cb' optional, it becomes escaping. So avoid that.
    return try _runSQL(sql: sql, optAttrs: nil, bindings: nil) { rec in } ?? 0
  }
  
  
  // MARK: - Values
  
  func valueForBinaryPGValue(type: Oid, value: UnsafeBufferPointer<Int8>,
                             attribute: Attribute?) -> Any?
  {
    // TODO: consider attribute! (e.g. for date, valueType in attr, if set)
    
    // TODO: decode actual types :-)
    
    switch type {
      case OIDs.INT2:    return Int16(bigEndian: cast(value.baseAddress!))
      case OIDs.INT4:    return Int32(bigEndian: cast(value.baseAddress!))
      case OIDs.INT8:    return Int64(bigEndian: cast(value.baseAddress!))
      
      // Float has no bigEndian
      //  case OIDs.FLOAT4:  return Float32(bigEndian: cast(value.baseAddress!))
      //  case OIDs.FLOAT8:  return Float64(bigEndian: cast(value.baseAddress!))
      
      case OIDs.BOOL:    return (value.baseAddress!.pointee != 0)
      
      case OIDs.VARCHAR, OIDs.TEXT: return String(cString: value.baseAddress!)
      
      case OIDs.NAME: // e.g. SELECT datname FROM pg_database
        return String(cString: value.baseAddress!)
      
      case OIDs.TIMESTAMPTZ:
        // TODO: I think it is better to fix this during the query, that is,
        // to a SELECT unix_time(startDate) like thing.
        // hm. How to parse this? We used to have the format in the attribute?
        // http://www.linuxtopia.org/online_books/database_guides/Practical_PostgreSQL_database/PostgreSQL_x2632_005.htm
        return String(cString: value.baseAddress!)
      
      case OIDs.OID:
        // https://www.postgresql.org/docs/9.5/static/datatype-oid.html
        return UInt32(bigEndian: cast(value.baseAddress!))
      
      default:
        print("OID: \(type): \(String(cString:value.baseAddress!))")
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
                    bindings: sqlexpr.bindVariables, cb: result)
  }

  public func evaluateUpdateExpression(_ sqlexpr: SQLExpression) throws -> Int {
    return try _runSQL(sql: sqlexpr.statement, optAttrs: nil,
                       bindings: sqlexpr.bindVariables) { rec in } ?? 0
  }
  
  
  // MARK: - Transactions
  
  public var isTransactionInProgress : Bool = false
  
  public func begin() throws {
    guard !isTransactionInProgress
     else { throw AdaptorChannelError.TransactionInProgress }
    
    try performSQL("BEGIN TRANSACTION;")
    isTransactionInProgress = true
  }
  public func commit() throws {
    isTransactionInProgress = false
    try performSQL("COMMIT TRANSACTION;")
  }
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
  
  public func describeSequenceNames() throws -> [ String ] {
    return try PostgreSQLModelFetch(channel: self).describeSequenceNames()
  }
  
  public func describeDatabaseNames() throws -> [ String ] {
    return try PostgreSQLModelFetch(channel: self).describeDatabaseNames()
  }
  public func describeTableNames() throws -> [ String ] {
    return try PostgreSQLModelFetch(channel: self).describeTableNames()
  }

  public func describeEntityWithTableName(_ table: String) throws -> Entity? {
    return try PostgreSQLModelFetch(channel: self)
                 .describeEntityWithTableName(table)
  }

  
  // MARK: - Insert w/ auto-increment support
  
  open func insertRow(_ row: AdaptorRow, _ entity: Entity?, refetchAll: Bool)
              throws -> AdaptorRow
  {
    let attributes : [ Attribute ]? = {
      guard let entity = entity else { return nil }
      
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
        throw AdaptorError.FailedToRefetchInsertedRow(
                             entity: entity, row: row)
      }
      rec = record
    }
    guard let rrec = rec else { // no record returned?
      throw AdaptorError.FailedToRefetchInsertedRow(entity: entity, row: row)
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
