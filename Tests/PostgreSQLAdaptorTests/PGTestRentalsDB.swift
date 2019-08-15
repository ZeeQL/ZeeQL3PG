//
//  PGTestRentalsDB.swift
//  PostgreSQLAdaptorTests
//
//  Created by Helge Heß on 15.08.19.
//  Copyright © 2019 ZeeZide GmbH. All rights reserved.
//

import XCTest
@testable import ZeeQL
@testable import PostgreSQLAdaptor

class PGTestRentalsDB: XCTestCase {

  // Assumes that the `dvdrental` database is configured:
  // http://www.postgresqltutorial.com/load-postgresql-sample-database/
  // createuser -s postgres
  // createdb dvdrental
  // cd dvdrental
  // pg_restore -h localhost -U postgres -d dvdrental .

  var adaptor : Adaptor! {
    XCTAssertNotNil(_adaptor)
    return _adaptor
  }

  let _adaptor = PostgreSQLAdaptor(database: "dvdrental")

  func testConnect() throws {
    let channel = try adaptor.openChannel()
    let results = try channel.querySQL("SELECT * FROM actor;")
    assert(results.count > 100)
  }

  func testConnect2() throws {
    let adaptor = PostgreSQLAdaptor(database: "dvdrental")
    let channel = try adaptor.openChannel()
    
    try channel.select("SELECT actor_id, first_name, last_name FROM actor;") {
      ( id: Int, firstName: String, lastName: String ) in
      print("\(id): \(firstName) \(lastName)")
    }
  }

  func testDescribeDatabaseNames() throws {
    let channel = try adaptor.openChannel()
    let values  = try channel.describeDatabaseNames()
    
    XCTAssert(values.count >= 4)
    XCTAssert(values.contains("template0"))
    XCTAssert(values.contains("template1"))
    XCTAssert(values.contains("postgres"))
    XCTAssert(values.contains("dvdrental"))
  }
  
  func testDescribeRentalTableNames() throws {
    let channel = try adaptor.openChannel()
    let values  = try channel.describeTableNames()
    
    XCTAssert(values.count >= 15) // 126 in my OGo2 DB with extras
    XCTAssert(values.contains("film_actor"))
    XCTAssert(values.contains("customer"))
    XCTAssert(values.contains("store"))
  }

  func testFetchModel() throws {
    let channel = try adaptor.openChannel()
    let model   = try PostgreSQLModelFetch(channel: channel).fetchModel()
    
    XCTAssert(model.entities.count >= 15)
    let values = model.entityNames
    XCTAssert(values.contains("film_actor"))
    XCTAssert(values.contains("customer"))
    XCTAssert(values.contains("store"))
    
    if let entity = model[entity: "actor"] {
      print("Actor:", entity)
      XCTAssert(entity.attributes.count == 4)
      
      XCTAssert(entity.primaryKeyAttributeNames?.count == 1)
      if let pkeyName = entity.primaryKeyAttributeNames?.first,
         let pkey = entity[attribute: pkeyName]
      {
        XCTAssertEqual(pkeyName, "actor_id")
        XCTAssertEqual(pkey.name, pkeyName)
        XCTAssertNotNil(pkey.allowsNull)
        if let allowsNull = pkey.allowsNull {
          XCTAssertFalse(allowsNull)
        }
      }
    }
    if let entity = model[entity: "film_actor"] {
      print("film_actor:", entity)
      XCTAssert(entity.attributes   .count == 3)
      XCTAssert(entity.relationships.count == 2)
      
      // film_actor_actor_id_fkey
      // FIXME: should be just actor?
      let relshipName = "film_actor_actor_id_fkey"
      XCTAssertNotNil(entity[relationship: relshipName])
      if let relship = entity[relationship: relshipName] {
        print("  =>actor:", relship)
        XCTAssertEqual(relship.entity.name,             "film_actor")
        XCTAssertEqual(relship.destinationEntity?.name, "actor")
        XCTAssertFalse(relship.isToMany)
        XCTAssert(relship.isMandatory)
        XCTAssertEqual(relship.joins.count, 1)
        if let join = relship.joins.first {
          XCTAssertEqual(join.sourceName,      "actor_id")
          XCTAssertEqual(join.destinationName, "actor_id")
          if !relship.isMandatory {
            print("    source:", join.source!)
            print("    dest:  ", join.destination!)
          }
        }
      }
    }

    XCTAssertNotNil(model.tag, "model has no tag")
  }

}
