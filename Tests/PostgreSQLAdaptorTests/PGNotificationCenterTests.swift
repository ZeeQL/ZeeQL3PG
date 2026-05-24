//
//  PGNotificationCenterTests.swift
//  ZeeQL3PG
//
//  Created by Helge Hess on 24/05/26.
//  Copyright © 2026 ZeeZide GmbH. All rights reserved.
//

import XCTest
import Foundation
@testable import PostgreSQLAdaptor

final class PGNotificationCenterTests: PGTestCase {

  private var center: PGNotificationCenter!

  override func setUpWithError() throws {
    try super.setUpWithError()
    center = try adaptor.openNotificationCenter()
  }

  override func tearDown() {
    center?.close()
    center = nil
    super.tearDown()
  }


  // MARK: - Tests

  func testReceivesNotificationWithPayload() throws {
    let channel = Self.uniqueChannel()
    let exp     = expectation(description: "receive notification")
    let sub     = CapturingSubscriber(receive: exp)

    try center.subscribe(sub, to: [ channel ])
    try sendNotify(channel, payload: "hello")

    wait(for: [ exp ], timeout: 5)

    XCTAssertEqual(sub.received.count, 1)
    XCTAssertEqual(sub.received.first?.channel, channel)
    XCTAssertEqual(sub.received.first?.payload, "hello")
    XCTAssertGreaterThan(sub.received.first?.pid ?? 0, 0)
  }

  func testReceivesNotificationWithoutPayload() throws {
    let channel = Self.uniqueChannel()
    let exp     = expectation(description: "receive without payload")
    let sub     = CapturingSubscriber(receive: exp)

    try center.subscribe(sub, to: [ channel ])
    try sendNotify(channel, payload: nil)

    wait(for: [ exp ], timeout: 5)
    XCTAssertEqual(sub.received.first?.payload, "")
  }

  func testTwoSubscribersOnSameChannelBothReceive() throws {
    let channel = Self.uniqueChannel()
    let exp1    = expectation(description: "sub1 receives")
    let exp2    = expectation(description: "sub2 receives")
    let sub1    = CapturingSubscriber(receive: exp1)
    let sub2    = CapturingSubscriber(receive: exp2)

    try center.subscribe(sub1, to: [ channel ])
    try center.subscribe(sub2, to: [ channel ])
    try sendNotify(channel, payload: "shared")

    wait(for: [ exp1, exp2 ], timeout: 5)
    XCTAssertEqual(sub1.received.first?.payload, "shared")
    XCTAssertEqual(sub2.received.first?.payload, "shared")
  }

  func testUnsubscribeStopsDelivery() throws {
    let channel = Self.uniqueChannel()
    let exp     = expectation(description: "first delivery")
    let sub     = CapturingSubscriber(receive: exp)

    try center.subscribe(sub, to: [ channel ])
    try sendNotify(channel, payload: "first")
    wait(for: [ exp ], timeout: 5)

    try center.unsubscribe(sub, from: [ channel ])
    sub.receiveExpectation = nil // disarm any further fulfillment
    try sendNotify(channel, payload: "second")

    // Give the second NOTIFY time to *not* arrive; LISTEN was dropped
    // before it was published.
    Thread.sleep(forTimeInterval: 0.5)
    XCTAssertEqual(sub.received.count, 1)
  }

  func testRefcountedUnsubscribeKeepsOtherSubscriber() throws {
    let channel = Self.uniqueChannel()
    let exp     = expectation(description: "sub2 still receives")
    let sub1    = CapturingSubscriber()
    let sub2    = CapturingSubscriber(receive: exp)

    try center.subscribe(sub1, to: [ channel ])
    try center.subscribe(sub2, to: [ channel ])
    try center.unsubscribe(sub1, from: [ channel ])
    try sendNotify(channel, payload: "after sub1 unsubscribed")

    wait(for: [ exp ], timeout: 5)
    XCTAssertEqual(sub1.received.count, 0)
    XCTAssertEqual(sub2.received.count, 1)
  }

  func testResubscribeSameSubscriberIsNoOp() throws {
    let channel = Self.uniqueChannel()
    let exp     = expectation(description: "single delivery")
    let sub     = CapturingSubscriber(receive: exp)

    try center.subscribe(sub, to: [ channel ])
    try center.subscribe(sub, to: [ channel ]) // duplicate
    try sendNotify(channel, payload: "once")

    wait(for: [ exp ], timeout: 5)
    XCTAssertEqual(sub.received.count, 1,
                   "callback must fire exactly once even after duplicate sub")
  }

  func testCloseFiresDisconnectExactlyOnce() throws {
    let channel = Self.uniqueChannel()
    let exp     = expectation(description: "disconnect")
    let sub     = CapturingSubscriber(disconnect: exp)

    try center.subscribe(sub, to: [ channel ])
    center.close()

    wait(for: [ exp ], timeout: 5)
    XCTAssertEqual(sub.disconnectCount, 1)
    XCTAssertTrue(center.isClosed)
  }

  func testListenerSurvivesBurstOfNotifications() throws {
    let channel  = Self.uniqueChannel()
    let count    = 50_000
    let burstExp = expectation(description: "burst of \(count)")
    burstExp.expectedFulfillmentCount = count
    burstExp.assertForOverFulfill     = true

    let sub = CapturingSubscriber(receive: burstExp)
    try center.subscribe(sub, to: [ channel ])

    let writer = try adaptor.openChannel()
    defer { adaptor.releaseChannel(writer) }

    // Fire all `count` notifications in a single round-trip; the server
    // queues them and delivers the lot on transaction commit.
    _ = try writer.performSQL(
      "SELECT pg_notify('\(channel)', i::text) " +
      "FROM generate_series(0, \(count - 1)) AS i"
    )

    wait(for: [ burstExp ], timeout: 60)
    XCTAssertEqual(sub.received.count, count)
    XCTAssertFalse(center.isClosed,
                   "listener should still be alive after burst")

    // Spot-check ordering: PG delivers notifications in commit order, so
    // payloads should be "0", "1", ... "count-1".
    XCTAssertEqual(sub.received.first?.payload, "0")
    XCTAssertEqual(sub.received.last?.payload, String(count - 1))

    // Listener must still be functional after the burst — send one more
    // and make sure it arrives.
    let aliveExp = expectation(description: "still alive after burst")
    sub.receiveExpectation = aliveExp
    _ = try writer.performSQL("NOTIFY \"\(channel)\", 'alive'")
    wait(for: [ aliveExp ], timeout: 5)
    XCTAssertEqual(sub.received.last?.payload, "alive")
    XCTAssertEqual(sub.received.count, count + 1)
  }

  func testPidMatchesWriterBackendID() throws {
    let channel = Self.uniqueChannel()
    let exp     = expectation(description: "receive")
    let sub     = CapturingSubscriber(receive: exp)

    try center.subscribe(sub, to: [ channel ])

    let writer = try adaptor.openChannel()
    defer { adaptor.releaseChannel(writer) }
    let writerPID = (writer as? PostgreSQLAdaptorChannel)?.backendProcessID
    XCTAssertNotNil(writerPID)
    try writer.performSQL("NOTIFY \"\(channel)\", 'pid-test'")

    wait(for: [ exp ], timeout: 5)
    XCTAssertEqual(sub.received.first?.pid, writerPID)
  }


  // MARK: - Helpers

  private func sendNotify(_ channel: String, payload: String?) throws {
    let ch = try adaptor.openChannel()
    defer { adaptor.releaseChannel(ch) }
    let sql: String
    if let payload {
      let escaped = payload.replacingOccurrences(of: "'", with: "''")
      sql = "NOTIFY \"\(channel)\", '\(escaped)'"
    }
    else {
      sql = "NOTIFY \"\(channel)\""
    }
    _ = try ch.performSQL(sql)
  }

  private static func uniqueChannel() -> String {
    let id = UUID().uuidString.lowercased()
              .replacingOccurrences(of: "-", with: "")
    return "pgnc_test_\(id)"
  }
}


// MARK: - Helper Subscriber

private final class CapturingSubscriber: PGNotificationSubscriber,
                                         @unchecked Sendable
{

  private let lock              = NSLock()
  private var _received         = [ PGNotification ]()
  private var _disconnectCount  = 0
  private var _disconnectError  : (any Swift.Error)?

  var receiveExpectation    : XCTestExpectation?
  var disconnectExpectation : XCTestExpectation?

  init(receive    : XCTestExpectation? = nil,
       disconnect : XCTestExpectation? = nil)
  {
    receiveExpectation    = receive
    disconnectExpectation = disconnect
  }

  var received : [ PGNotification ] {
    lock.lock(); defer { lock.unlock() }
    return _received
  }
  var disconnectCount : Int {
    lock.lock(); defer { lock.unlock() }
    return _disconnectCount
  }
  var disconnectError : (any Swift.Error)? {
    lock.lock(); defer { lock.unlock() }
    return _disconnectError
  }

  func notificationCenter(_ center: PGNotificationCenter,
                          didReceive notification: PGNotification)
  {
    lock.lock()
    _received.append(notification)
    let exp = receiveExpectation
    lock.unlock()
    exp?.fulfill()
  }

  func notificationCenter(_ center: PGNotificationCenter,
                          didDisconnectWithError error: (any Swift.Error)?)
  {
    lock.lock()
    _disconnectCount += 1
    _disconnectError = error
    let exp = disconnectExpectation
    lock.unlock()
    exp?.fulfill()
  }
}
