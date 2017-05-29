//
//  PostgreSQLOIDs.swift
//  ZeeQL3PG
//
//  Created by Helge Hess on 10/04/17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import typealias CLibPQ.Oid

// Just an internal holder, the client should not be concerned about this. If
// needed, we should have higher level type IDs used by all adaptors.

enum OIDs {
  // the ones from SOPE
  
  static let BOOL        : Oid = 16
  static let BYTEA       : Oid = 17
  static let CHAR        : Oid = 18
  static let NAME        : Oid = 19
  static let INT8        : Oid = 20
  static let INT2        : Oid = 21
  static let INT2VECTOR  : Oid = 22
  static let INT4        : Oid = 23
  static let REGPROC     : Oid = 24
  static let TEXT        : Oid = 25
  static let OID         : Oid = 26
  static let TID         : Oid = 27
  static let XID         : Oid = 28
  static let CID         : Oid = 29
  static let OIDVECTOR   : Oid = 30
  static let POINT       : Oid = 600
  static let LSEG        : Oid = 601
  static let PATH        : Oid = 602
  static let BOX         : Oid = 603
  static let POLYGON     : Oid = 604
  static let LINE        : Oid = 628
  static let FLOAT4      : Oid = 700
  static let FLOAT8      : Oid = 701
  static let ABSTIME     : Oid = 702
  static let RELTIME     : Oid = 703
  static let TINTERVAL   : Oid = 704
  static let UNKNOWN     : Oid = 705
  static let CIRCLE      : Oid = 718
  static let CASH        : Oid = 790
  static let MACADDR     : Oid = 829
  static let INET        : Oid = 869
  static let CIDR        : Oid = 650
  static let ACLITEMS    : Oid = 8
  static let BPCHAR      : Oid = 1042
  static let VARCHAR     : Oid = 1043
  static let DATE        : Oid = 1082
  static let TIME        : Oid = 1083
  static let TIMESTAMP   : Oid = 1114
  static let TIMESTAMPTZ : Oid = 1184
  static let INTERVAL    : Oid = 1186
  static let TIMETZ      : Oid = 1266
  static let BIT         : Oid = 1560
  static let VARBIT      : Oid = 1562
  static let NUMERIC     : Oid = 1700
  static let REFCURSOR   : Oid = 1790
}

