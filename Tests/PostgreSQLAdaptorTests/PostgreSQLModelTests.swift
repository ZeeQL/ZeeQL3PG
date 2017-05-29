//
//  PostgreSQLModelTests.swift
//  ZeeQL3PG
//
//  Created by Helge Hess on 15.04.17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import XCTest
@testable import ZeeQL
@testable import PostgreSQLAdaptor

class PostgreSQLModelTests: XCTestCase {
  
  var adaptor : Adaptor! {
    XCTAssertNotNil(_adaptor)
    return _adaptor
  }

  let _adaptor = PostgreSQLAdaptor(database: "OGo2",
                                   user: "OGo", password: "OGo")

  func testDescribeDatabaseNames() throws {
    let channel = try adaptor.openChannel()
    let values  = try channel.describeDatabaseNames()
    
    XCTAssert(values.count >= 4)
    XCTAssert(values.contains("template0"))
    XCTAssert(values.contains("template1"))
    XCTAssert(values.contains("postgres"))
    XCTAssert(values.contains("OGo2"))
  }
  
  func testDescribeOGoTableNames() throws {
    let channel = try adaptor.openChannel()
    let values  = try channel.describeTableNames()
    
    XCTAssert(values.count >= 63) // 126 in my OGo2 DB with extras
    XCTAssert(values.contains("date_x")) // yes, lame
    XCTAssert(values.contains("person"))
    XCTAssert(values.contains("address"))
  }
  
  func testFetchModel() throws {
    let channel = try adaptor.openChannel()
    let model   = try PostgreSQLModelFetch(channel: channel).fetchModel()
    
    XCTAssert(model.entities.count >= 63)
    let values = model.entityNames
    XCTAssert(values.contains("date_x")) // yes, lame
    XCTAssert(values.contains("person"))
    XCTAssert(values.contains("address"))
    
    XCTAssertNotNil(model.tag, "model has no tag")
  }
  
  func testSchemaTag() throws {
    let channel    = try adaptor.openChannel()
    let modelFetch = PostgreSQLModelFetch(channel: channel)
    
    try channel.performSQL("DROP TABLE IF EXISTS zeeqltesttag")
    defer {
      _ = try? channel.performSQL("DROP TABLE IF EXISTS zeeqltesttag")
    }
    
    let tag1 = try modelFetch.fetchModelTag()
    let tag2 = try modelFetch.fetchModelTag()
    XCTAssert(eq(tag1, tag2))
    
    try channel.performSQL("CREATE TABLE zeeqltesttag ( id INT )")
    let tag3 = try modelFetch.fetchModelTag()
    XCTAssertFalse(eq(tag2, tag3))
    XCTAssertFalse(eq(tag1, tag3))
    
    let tag4 = try modelFetch.fetchModelTag()
    XCTAssert(eq(tag3, tag4))
    
    try channel.performSQL("ALTER TABLE zeeqltesttag ADD COLUMN name TEXT")
    let tag5 = try modelFetch.fetchModelTag()
    XCTAssertFalse(eq(tag4, tag5))
    XCTAssertFalse(eq(tag1, tag5))

    try channel.performSQL("DROP TABLE zeeqltesttag")
    let tag6 = try modelFetch.fetchModelTag()
    XCTAssertFalse(eq(tag5, tag6))
    XCTAssertFalse(eq(tag4, tag6))
    // XCTAssert(eq(tag1, tag6)) // this is not necessarily true, since we hash
    // we can get the same key again! But there is no guarantee for that.
  }
}
