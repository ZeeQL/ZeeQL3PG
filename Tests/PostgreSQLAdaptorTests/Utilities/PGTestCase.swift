//
//  PGTestCase.swift
//  ZeeQL3PG
//
//  Base class for tests that need a live PostgreSQL connection.
//  Skips all tests in the class when no PG is reachable, caching the
//  probe result so we don't reconnect for every test method.
//
//  Created by Helge Hess on 24/05/26.
//  Copyright © 2026 ZeeZide GmbH. All rights reserved.
//

import XCTest
@testable import PostgreSQLAdaptor

class PGTestCase: XCTestCase {

  nonisolated(unsafe)
  private static var _available : Bool?

  static let adaptor : PostgreSQLAdaptor = .testMake()

  var adaptor : PostgreSQLAdaptor { Self.adaptor }

  override func setUpWithError() throws {
    try super.setUpWithError()

    if let available = Self._available {
      try XCTSkipUnless(available, Self.skipMessage)
      return
    }

    do {
      let channel = try Self.adaptor.openChannel()
      Self.adaptor.releaseChannel(channel)
      Self._available = true
    }
    catch {
      Self._available = false
      throw XCTSkip("\(Self.skipMessage) Error: \(error)")
    }
  }

  private static let skipMessage =
    "No PostgreSQL reachable — set PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD."
}
