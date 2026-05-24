//
//  PGNotificationCenter.swift
//  ZeeQL
//
//  Created by Helge Hess on 21/05/26.
//  Copyright © 2026 ZeeZide GmbH. All rights reserved.
//

import Foundation
import Dispatch
import ZeeQL
import CLibPQ

/**
 * A notification delivered by the PostgreSQL server via `NOTIFY`/`pg_notify`.
 *
 * The ``pid`` identifies the backend that called `NOTIFY` (i.e. one of your
 * write connections, *not* the listening connection). To filter out
 * notifications caused by writes the current process performed itself,
 * compare against ``PostgreSQLAdaptorChannel/backendProcessID`` of the
 * write channels in use.
 *
 * - ``channel``: The channel name (the first argument to `NOTIFY`)
 * - ``payload``: The optional payload (the second argument), can be empty.
 * - ``pid``:     The backend PID of the PG process that sent the notification.
 */
public struct PGNotification: Hashable, Sendable {

  public let channel : String
  public let payload : String
  public let pid     : Int32

  @inlinable
  public init(channel: String, payload: String, pid: Int32) {
    self.channel = channel
    self.payload = payload
    self.pid     = pid
  }
}

/**
 * Receives ``PGNotification``s delivered by a ``PGNotificationCenter``
 * the subscriber is registered with.
 *
 * Methods are invoked on the center's delivery queue (a private serial
 * queue by default, or the queue supplied at construction time).
 */
public protocol PGNotificationSubscriber: AnyObject, Sendable {

  /// Called for every notification received on a channel the subscriber
  /// is subscribed to. May be called from any thread, but never
  /// concurrently for the same subscriber (delivery is serialized on
  /// the center's delivery queue).
  func notificationCenter(_ center: PGNotificationCenter,
                          didReceive notification: PGNotification)

  /// Called once when the underlying PG connection is lost or the
  /// center is closed. After this, the subscriber's view of the world
  /// is stale and a full resync should be performed before
  /// re-subscribing on a new center.
  ///
  /// The default implementation is a no-op.
  func notificationCenter(_ center: PGNotificationCenter,
                          didDisconnectWithError error: (Swift.Error)?)
}

public extension PGNotificationSubscriber {

  func notificationCenter(_ center: PGNotificationCenter,
                          didDisconnectWithError error: (Swift.Error)?) {}
}


/**
 * An object that can listen to PostgreSQL notifications.
 *
 * Owns a dedicated PostgreSQL connection that runs `LISTEN`/`UNLISTEN`
 * and fans incoming `NOTIFY` messages out to subscribers.
 *
 * The center is acquired from a ``PostgreSQLAdaptor`` via
 * ``PostgreSQLAdaptor/openNotificationCenter(deliveryQueue:)``. The
 * underlying connection is *not* taken from the channel pool; it is a
 * dedicated, long-lived PGconn because `LISTEN` registrations are
 * connection-scoped.
 *
 * ### Subscriber lifecycle
 *
 * Subscribers are held *weakly*. A subscriber that is deallocated is
 * silently dropped on the next subscribe/unsubscribe of the same
 * channel.
 *
 * ### Channel reference counting
 *
 * The center reference-counts channels internally. `LISTEN` is issued
 * the first time a channel acquires a subscriber, `UNLISTEN` the last
 * time it loses one. Callers don't need to coordinate.
 *
 * ### Connection loss
 *
 * If the PG connection drops or a libpq error is reported by
 * `PQconsumeInput`, the center fires
 * ``PGNotificationSubscriber/notificationCenter(_:didDisconnectWithError:)``
 * on every subscriber once and shuts itself down. There is no
 * auto-reconnect — open a new center.
 */
public final class PGNotificationCenter: SmartDescription, @unchecked Sendable {

  private struct WeakSubscriber {
    weak var subscriber: (any PGNotificationSubscriber)?
  }

  public let adaptor       : PostgreSQLAdaptor
  public let deliveryQueue : DispatchQueue

  private let ioQueue : DispatchQueue
  private let quoter  = PostgreSQLExpression() // used for identifier quoting
  // Following are accessed only from `ioQueue`:
  private var handle       : OpaquePointer?
  private var socketSource : (any DispatchSourceRead)?
  private var subscribers  : [ String : [ WeakSubscriber ] ] = [:]
  private var closed       : Bool = false

  init(adaptor: PostgreSQLAdaptor, handle: OpaquePointer,
       deliveryQueue: DispatchQueue?) throws
  {
    self.adaptor       = adaptor
    self.handle        = handle
    self.ioQueue       =
      DispatchQueue(label: "de.zeezide.PGNotificationCenter.io")
    self.deliveryQueue = deliveryQueue ?? .main

    let fd = PQsocket(handle)
    guard fd >= 0 else {
      PQfinish(handle)
      self.handle = nil
      throw PostgreSQLAdaptorError.couldNotConnect("PQsocket() returned -1")
    }
    let source = DispatchSource
      .makeReadSource(fileDescriptor: fd, queue: ioQueue)
    source.setEventHandler { [weak self] in self?.drainOnIO() }
    source.resume()
    self.socketSource = source
  }

  deinit {
    // Cannot dispatch_sync onto ioQueue from deinit safely; the source
    // has captured `[weak self]`, so once we're here no more events
    // will reach us. Just release the underlying resources.
    socketSource?.cancel()
    socketSource = nil
    if let handle = handle { PQfinish(handle) }
    handle = nil
  }

  /// `true` once the center has been closed (either explicitly via
  /// ``close()`` or implicitly by losing the PG connection).
  public var isClosed : Bool {
    return ioQueue.sync { closed }
  }


  // MARK: - Subscription

  /**
   * Subscribe `subscriber` to one or more channels.
   *
   * The first subscriber for a channel triggers `LISTEN <channel>` on
   * the dedicated connection. Subsequent subscribers reuse the existing
   * registration. The subscriber is held weakly.
   *
   * Re-subscribing the same subscriber to a channel it already receives
   * is a no-op.
   *
   * - Throws: ``PostgreSQLAdaptorError/notificationCenterClosed`` if the
   *           center has been closed,
   *           ``PostgreSQLAdaptorError/listenFailed(channel:reason:)`` if
   *           `LISTEN` failed.
   */
  public func subscribe(_ subscriber: any PGNotificationSubscriber,
                        to channels: Set<String>) throws
  {
    guard !channels.isEmpty else { return }
    try ioQueue.sync {
      guard !closed, let handle = handle else { throw PostgreSQLAdaptorError.notificationCenterClosed }

      var toListen = [ String ]()
      for channel in channels {
        var arr = subscribers[channel, default: []]
        arr.removeAll { $0.subscriber == nil }
        let wasEmpty = arr.isEmpty
        let already  = arr.contains { $0.subscriber === subscriber }
        if !already { arr.append(WeakSubscriber(subscriber: subscriber)) }
        subscribers[channel] = arr
        if wasEmpty { toListen.append(channel) }
      }

      for ch in toListen {
        try execute(handle: handle, sql: "LISTEN \(quoter.sqlStringFor(schemaObjectName: ch))",
                    failAs: { PostgreSQLAdaptorError.listenFailed(channel: ch, reason: $0) })
      }
      // Drain any notifications that may have arrived while running the
      // LISTEN statements above.
      drainOnIO()
    }
  }

  /**
   * Unsubscribe `subscriber` from `channels`, or from *all* channels it
   * is currently subscribed to when `channels` is `nil`.
   *
   * `UNLISTEN <channel>` is issued for any channel that loses its last
   * subscriber. Unsubscribing a subscriber that wasn't registered is a
   * no-op.
   *
   * - Throws: ``PostgreSQLAdaptorError/notificationCenterClosed`` if the
   *           center has been closed;
   *           ``PostgreSQLAdaptorError/unlistenFailed(channel:reason:)`` if
   *           `UNLISTEN` failed.
   */
  public func unsubscribe(_ subscriber: any PGNotificationSubscriber,
                          from channels: Set<String>? = nil) throws
  {
    try ioQueue.sync {
      guard !closed, let handle = handle else { throw PostgreSQLAdaptorError.notificationCenterClosed }

      let target = channels ?? Set(subscribers.keys)
      var toUnlisten = [ String ]()
      for channel in target {
        guard var arr = subscribers[channel] else { continue }
        arr.removeAll { $0.subscriber == nil || $0.subscriber === subscriber }
        if arr.isEmpty {
          subscribers[channel] = nil
          toUnlisten.append(channel)
        }
        else {
          subscribers[channel] = arr
        }
      }

      for ch in toUnlisten {
        try execute(handle: handle, sql: "UNLISTEN \(quoter.sqlStringFor(schemaObjectName: ch))",
                    failAs: { PostgreSQLAdaptorError.unlistenFailed(channel: ch, reason: $0) })
      }
    }
  }

  public func close() {
    ioQueue.sync { _closeOnIO(notifyError: nil) }
  }


  // MARK: - I/O (all on ioQueue)

  private func drainOnIO() {
    guard let handle = handle, !closed else { return }

    if PQconsumeInput(handle) == 0 {
      let msg = PQerrorMessage(handle).map { String(cString: $0) }
             ?? "PQconsumeInput failed."
      _closeOnIO(notifyError: PostgreSQLAdaptorError.couldNotConnect(msg))
      return
    }

    var grouped = [ String : [ PGNotification ] ]()
    while let notifPtr = PQnotifies(handle) {
      let channel = notifPtr.pointee.relname.map { String(cString: $0) } ?? ""
      let payload = notifPtr.pointee.extra  .map { String(cString: $0) } ?? ""
      let pid     = notifPtr.pointee.be_pid
      PQfreemem(UnsafeMutableRawPointer(notifPtr))
      grouped[channel, default: []]
        .append(PGNotification(channel: channel, payload: payload, pid: pid))
    }

    if !grouped.isEmpty { dispatchBatchOnIO(grouped) }

    if PQstatus(handle) == CONNECTION_BAD {
      let msg = PQerrorMessage(handle).map { String(cString: $0) }
             ?? "Connection lost."
      _closeOnIO(notifyError: PostgreSQLAdaptorError.couldNotConnect(msg))
    }
  }

  private func dispatchBatchOnIO(_ grouped: [ String : [ PGNotification ] ]) {
    // Snapshot live subscribers per channel under `ioQueue` so the
    // delivery closure doesn't have to touch shared state.
    var batches = [ (subs          : [ any PGNotificationSubscriber ],
                     notifications : [ PGNotification ]) ]()
    batches.reserveCapacity(grouped.count)
    for (channel, notifs) in grouped {
      let subs = (subscribers[channel] ?? []).compactMap(\.subscriber)
      if subs.isEmpty { continue }
      batches.append((subs, notifs))
    }
    guard !batches.isEmpty else { return }

    let center = self
    deliveryQueue.async {
      for (subs, notifs) in batches {
        for notification in notifs {
          for sub in subs {
            sub.notificationCenter(center, didReceive: notification)
          }
        }
      }
    }
  }

  private func _closeOnIO(notifyError: (any Swift.Error)?) {
    guard !closed else { return }
    closed = true

    socketSource?.cancel()
    socketSource = nil
    if let handle = handle { PQfinish(handle) }
    handle = nil

    var seen = Set<ObjectIdentifier>()
    var allSubs = [ any PGNotificationSubscriber ]()
    for entries in subscribers.values {
      for entry in entries {
        guard let sub = entry.subscriber else { continue }
        if seen.insert(ObjectIdentifier(sub)).inserted { allSubs.append(sub) }
      }
    }
    subscribers.removeAll()

    guard !allSubs.isEmpty else { return }
    let center = self
    deliveryQueue.async {
      for sub in allSubs {
        sub.notificationCenter(center, didDisconnectWithError: notifyError)
      }
    }
  }

  private func execute(handle: OpaquePointer, sql: String,
                       failAs: (String) -> Error) throws
  {
    guard let res = PQexec(handle, sql) else {
      let msg = PQerrorMessage(handle).map { String(cString: $0) }
             ?? "PQexec returned NULL."
      throw failAs(msg)
    }
    defer { PQclear(res) }

    guard PQresultStatus(res) == PGRES_COMMAND_OK else {
      let msg = PQresultErrorMessage(res).map { String(cString: $0) }
             ?? "Unknown PG error."
      throw failAs(msg)
    }
  }


  // MARK: - Description

  public func appendToDescription(_ ms: inout String) {
    ioQueue.sync {
      if closed {
        ms += " closed"
      }
      else if let handle = handle {
        ms += " #\(PQbackendPID(handle))"
      }
      else {
        ms += " open-no-handle?"
      }
      ms += " subs=#\(subscribers.count)"
    }
  }
}

public extension PostgreSQLAdaptor {

  /**
   * Open a dedicated PG connection for LISTEN/NOTIFY and return a
   * ``PGNotificationCenter`` that owns it.
   *
   * The returned center is *not* part of the adaptor's channel pool and is not
   * shared between callers.
   * Close it with ``PGNotificationCenter/close()`` when done (or just release
   * the last reference).
   *
   * - Parameter deliveryQueue: Queue for subscriber callbacks.
   */
  func openNotificationCenter(deliveryQueue: DispatchQueue? = nil) throws
       -> PGNotificationCenter
  {
    guard let handle = PQconnectdb(connectString) else {
      let msg = PQerrorMessage(nil).map { String(cString: $0) }
             ?? "Got no handle from PQconnectdb."
      throw AdaptorError
        .couldNotOpenChannel(PostgreSQLAdaptorError.couldNotConnect(msg))
    }
    guard PQstatus(handle) == CONNECTION_OK else {
      let msg = PQerrorMessage(handle).map { String(cString: $0) }
             ?? "Not connected, no specific error."
      PQfinish(handle)
      throw AdaptorError
        .couldNotOpenChannel(PostgreSQLAdaptorError.couldNotConnect(msg))
    }
    do {
      return try PGNotificationCenter(adaptor: self, handle: handle,
                                      deliveryQueue: deliveryQueue)
    }
    catch {
      throw error
    }
  }
}
