//
//  PostgreSQLValueDecodingTests.swift
//  ZeeQL3PG
//
//  Created by Helge Heß on 06/07/26.
//  Copyright © 2026 ZeeZide GmbH. All rights reserved.
//

import XCTest
import Foundation
import ZeeQL
@testable import PostgreSQLAdaptor

final class PostgreSQLValueDecodingTests: PGTestCase {

  /// Run `sql` and return the single value of its first row's column `v`.
  private func scalar(_ sql: String) throws -> Any? {
    let channel = try adaptor.openChannel()
    defer { adaptor.releaseChannel(channel) }
    var rows = [ AdaptorRecord ]()
    try channel.querySQL(sql, nil) { rows.append($0) }
    let row = try XCTUnwrap(rows.first, "expected one row for: \(sql)")
    return row["v"]
  }


  // MARK: - Floating point (the load-bearing fix)

  func testFloat8Simple() throws {
    XCTAssertEqual(try scalar("SELECT 1.5::float8 AS v")     as? Double, 1.5)
    XCTAssertEqual(try scalar("SELECT (-3.75)::float8 AS v") as? Double, -3.75)
    XCTAssertEqual(try scalar("SELECT 0::float8 AS v")       as? Double, 0)
  }

  func testFloat8PreservesFullPrecisionBits() throws {
    // The closest Double to pi — verifies the bit-pattern reinterpret is
    // exact, not just approximately right.
    let v = try XCTUnwrap(try scalar("SELECT 3.141592653589793::float8 AS v")
                          as? Double)
    XCTAssertEqual(v.bitPattern, Double.pi.bitPattern)
  }

  func testFloat4Simple() throws {
    XCTAssertEqual(try scalar("SELECT 1.5::float4 AS v") as? Float, 1.5)
    XCTAssertEqual(try scalar("SELECT (-3.75)::float4 AS v") as? Float, -3.75)
  }

  func testFloat4Pi() throws {
    let v = try XCTUnwrap(try scalar("SELECT pi()::float4 AS v") as? Float)
    XCTAssertEqual(v, Float(Double.pi), accuracy: 1e-6)
  }


  // MARK: - JSON / JSONB

  func testJSONBObjectStripsVersionHeaderAndParses() throws {
    let d = try XCTUnwrap(
      try scalar(#"SELECT '{"a": 1, "b": [2, 3]}'::jsonb AS v"#) as? Data)
    // Must be valid JSON `Data` (i.e. the 0x01 version prefix was stripped).
    let obj  = try JSONSerialization.jsonObject(with: d)
    let dict = try XCTUnwrap(obj as? [String: Any])
    XCTAssertEqual(dict["a"] as? Int, 1)
    XCTAssertEqual(dict["b"] as? [Int], [ 2, 3 ])
  }

  func testJSONBArray() throws {
    let d = try XCTUnwrap(try scalar("SELECT '[1,2,3]'::jsonb AS v") as? Data)
    let arr = try JSONSerialization.jsonObject(with: d) as? [Int]
    XCTAssertEqual(arr, [ 1, 2, 3 ])
  }

  func testJSONPreservesText() throws {
    // `json` (not `jsonb`) keeps the exact source text, no header.
    let d = try XCTUnwrap(try scalar(#"SELECT '{"a":1}'::json AS v"#) as? Data)
    XCTAssertEqual(String(decoding: d, as: UTF8.self), #"{"a":1}"#)
  }


  // MARK: - UUID

  func testUUID() throws {
    let str = "11223344-5566-7788-99AA-BBCCDDEEFF00"
    let v   = try scalar("SELECT '\(str)'::uuid AS v")
    XCTAssertEqual(v as? UUID, UUID(uuidString: str))
  }


  // MARK: - Numeric => Decimal

  func testNumericFractional() throws {
    XCTAssertEqual(try scalar("SELECT 12345.6789::numeric AS v") as? Decimal,
                   Decimal(string: "12345.6789"))
  }

  func testNumericNegativeAndSmall() throws {
    XCTAssertEqual(try scalar("SELECT (-0.001)::numeric AS v") as? Decimal,
                   Decimal(string: "-0.001"))
  }

  func testNumericInteger() throws {
    XCTAssertEqual(try scalar("SELECT 100::numeric AS v") as? Decimal,
                   Decimal(100))
  }

  func testNumericZero() throws {
    XCTAssertEqual(try scalar("SELECT 0::numeric AS v") as? Decimal, Decimal(0))
  }

  func testNumericLargeMultiGroup() throws {
    // Spans several base-10000 digit groups on both sides of the point.
    let lit = "123456789012.34567890"
    XCTAssertEqual(try scalar("SELECT \(lit)::numeric AS v") as? Decimal,
                   Decimal(string: lit))
  }

  func testNumericNaNIsNil() throws {
    // NaN is not representable as a finite Decimal => decoder returns nil.
    XCTAssertNil(try scalar("SELECT 'NaN'::numeric AS v"))
  }


  // MARK: - Timestamps

  func testTimestampWithoutZoneDecodesToDate() throws {
    // Was mis-read as a String in binary mode; now a Date (interpreted UTC).
    let v = try XCTUnwrap(
      try scalar("SELECT '2026-05-29 12:29:00'::timestamp AS v") as? Date)
    var utc = Calendar(identifier: .gregorian)
    utc.timeZone = try XCTUnwrap(TimeZone(identifier: "UTC"))
    let expected = try XCTUnwrap(utc.date(from: DateComponents(
      year: 2026, month: 5, day: 29, hour: 12, minute: 29, second: 0)))
    XCTAssertEqual(v.timeIntervalSince1970, expected.timeIntervalSince1970,
                   accuracy: 0.001)
  }

  func testTimestamptzStillDecodesToDate() throws {
    let v = try scalar("SELECT '2026-05-29 12:29:00+00'::timestamptz AS v")
    XCTAssertNotNil(v as? Date)
  }
}
