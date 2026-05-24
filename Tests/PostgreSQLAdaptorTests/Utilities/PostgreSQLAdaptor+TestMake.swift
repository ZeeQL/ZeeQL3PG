//
//  PostgreSQLAdaptor+TestMake.swift
//  ZeeQL3PG
//
//  Created by Helge Hess on 24/05/26.
//  Copyright © 2026 ZeeZide GmbH. All rights reserved.
//

import Foundation
@testable import PostgreSQLAdaptor

extension PostgreSQLAdaptor {

  /**
   * Convenience factory that builds a ``PostgreSQLAdaptor`` from the
   * standard libpq environment variables, falling back to sensible
   * defaults when they are unset.
   *
   * Honored env vars (with defaults):
   * - `PGHOST`     (default: `127.0.0.1`)
   * - `PGPORT`     (default: `5432`)
   * - `PGDATABASE` (default: `postgres`)
   * - `PGUSER`     (default: `postgres`)
   * - `PGPASSWORD` (default: empty)
   */
  static func testMake() -> PostgreSQLAdaptor {
    let env = ProcessInfo.processInfo.environment
    return PostgreSQLAdaptor(
      host     : env["PGHOST"]                        ?? "127.0.0.1",
      port     : env["PGPORT"].flatMap(Int.init)      ?? 5432,
      database : env["PGDATABASE"]                    ?? "postgres",
      user     : env["PGUSER"]                        ?? "postgres",
      password : env["PGPASSWORD"]                    ?? ""
    )
  }
}
