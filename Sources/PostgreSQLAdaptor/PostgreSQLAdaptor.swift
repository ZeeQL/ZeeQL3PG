//
//  PostgreSQLAdaptor.swift
//  ZeeQL
//
//  Created by Helge Hess on 03/03/17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

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
