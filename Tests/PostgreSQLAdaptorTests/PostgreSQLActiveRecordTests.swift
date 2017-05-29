//
//  PostgreSQLActiveRecordTests.swift
//  ZeeQL3PG
//
//  Created by Helge Hess on 18/05/17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import Foundation
import XCTest
@testable import ZeeQL
@testable import PostgreSQLAdaptor

class PostgreSQLActiveRecordTests: AdapterActiveRecordTests {
  
  override var adaptor : Adaptor! { return _adaptor }
  var _adaptor : Adaptor = {
    return PostgreSQLAdaptor(database: "contacts",
                             user: "OGo", password: "OGo")
  }()
  
}
