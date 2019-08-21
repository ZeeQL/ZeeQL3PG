//
//  PostgreSQLAdaptor.swift
//  ZeeQL
//
//  Created by Helge Hess on 03/03/17.
//  Copyright © 2017-2019 ZeeZide GmbH. All rights reserved.
//

import Foundation
import ZeeQL
import CLibPQ

open class PostgreSQLAdaptor : Adaptor, SmartDescription {
  // TODO: Pool. We really need one for PG. (well, not for ApacheExpress which
  //       should use mod_dbd)
  
  public enum Error : Swift.Error {
    case Generic
    case NotImplemented
    case CouldNotConnect(String)
  }
  
  /// The connectString the adaptor was configured with.
  open var connectString : String
  
  /**
   * Configure the adaptor with the given connect string.
   *
   * A connect string can be a URL like:
   *
   *     postgresql://OGo:OGo@localhost:5432/OGo
   *
   * or a space separated list of parameters, like:
   *
   *     host=localhost port=5432 dbname=OGo user=OGo
   *
   * Common parameters
   *
   * - host
   * - port
   * - dbname
   * - user
   * - password
   *
   * The full capabilities are listed in the
   * [PostgreSQL docs](https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-CONNSTRING).
   *
   * Note: The init doesn't validate the connect string, if it is malformed,
   *       channel creation will fail.
   */
  public init(_ connectString: String) {
    // TODO: could call PQconninfoParse(constr, &error) to validate
    self.connectString = connectString
  }
  
  /**
   * Configure the adaptor with the given values.
   *
   * Example:
   *
   *     let adaptor = PostgreSQLAdaptor(database: "OGo", 
   *                                     user: "OGo", password: "OGo")
   */
  public convenience init(host: String = "127.0.0.1", port: Int = 5432,
                          database: String = "postgres",
                          user: String = "postgres",  password: String = "")
  {
    var s = ""
    if !host.isEmpty     { s += " host=\(host)"         }
    if port > 0          { s += " port=\(port)"         }
    if !database.isEmpty { s += " dbname=\(database)"   }
    if !user.isEmpty     { s += " user=\(user)"         }
    if !password.isEmpty { s += " password=\(password)" }
    self.init(s)
  }
  
  private func parseConnectionString(_ s: String) -> [ String : String ] {
    guard !s.isEmpty else { return [:] }
    let pairs : [ ( Substring, Substring ) ]
        = connectString.split(maxSplits: 32, omittingEmptySubsequences: true) {
          guard $0.unicodeScalars.count == 1 else { return false }
          return CharacterSet.whitespacesAndNewlines
                   .contains($0.unicodeScalars.first!)
        }
        .compactMap {
          let pair = $0.split(separator: "=", maxSplits: 1)
          guard pair.count == 2 else { return nil }
          return ( pair[0], pair[1] )
        }
    guard !pairs.isEmpty else { return [:] }
    var values = [ String : String ]()
    values.reserveCapacity(pairs.count)
    for ( key, value ) in pairs {
      values[String(key).lowercased()] = String(value)
    }
    return values
  }
  
  public var url: URL? {
    if connectString.hasPrefix("postgresql") && connectString.contains("://") {
      return URL(string: connectString)
    }
    
    let cfg = parseConnectionString(connectString)
    var url    = URLComponents()
    url.scheme = "postgresql"
    url.port   = cfg["port"].flatMap(Int.init)
    
    if let v = cfg["host"],     !v.isEmpty { url.host     = v }
    if let v = cfg["user"],     !v.isEmpty { url.user     = v }
    if let v = cfg["password"], !v.isEmpty { url.password = v }
    if let v = cfg["dbname"], !v.isEmpty {
      url.path = "/"
        + v.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed)!
    }
    return url.url
  }

  
  // MARK: - Support
  
  public var expressionFactory : SQLExpressionFactory
                               = PostgreSQLExpressionFactory.shared
  public var model             : Model? = nil
  
  
  // MARK: - Channels

  open func openChannel() throws -> AdaptorChannel {
    guard let handle = PQconnectdb(connectString) else {
      if let cstr = PQerrorMessage(nil) { // TBD
        throw AdaptorError.CouldNotOpenChannel(
                             Error.CouldNotConnect(String(cString: cstr)))
      }
      throw AdaptorError.CouldNotOpenChannel(
                             Error.CouldNotConnect("Got no handle?"))
    }
    
    guard PQstatus(handle) == CONNECTION_OK else {
      let reason : String
      if let cstr = PQerrorMessage(handle) { reason = String(cString: cstr) }
      else { reason = "Not connected, no specific error." }
      PQfinish(handle)
      throw AdaptorError.CouldNotOpenChannel(Error.CouldNotConnect(reason))
    }
    
    return PostgreSQLAdaptorChannel(adaptor: self, handle: handle)
  }
  
  public func releaseChannel(_ channel: AdaptorChannel) {
    // not maintaing a pool
  }
  
  
  // MARK: - Model
  
  public func fetchModel() throws -> Model {
    let channel = try openChannelFromPool()
    defer { releaseChannel(channel) }
    
    return try PostgreSQLModelFetch(channel: channel).fetchModel()
  }
  public func fetchModelTag() throws -> ModelTag {
    let channel = try openChannelFromPool()
    defer { releaseChannel(channel) }
    
    return try PostgreSQLModelFetch(channel: channel).fetchModelTag()
  }
  
  
  // MARK: - Description

  public func appendToDescription(_ ms: inout String) {
    ms += " " + connectString
    if model != nil {
      ms += " has-model"
    }
  }
}
