//
//  APRAdaptorOGoTests.swift
//  ZeeQL
//
//  Created by Helge Hess on 24/02/17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import XCTest
@testable import ZeeQL
@testable import PostgreSQLAdaptor

class PostgreSQLAdaptorOGoTests: AdaptorOGoTestCase {
  
  override var adaptor : Adaptor! {
    XCTAssertNotNil(_adaptor)
    return _adaptor
  }

  let _adaptor = PostgreSQLAdaptor(database: "OGo2",
                                   user: "OGo", password: "OGo")
}

