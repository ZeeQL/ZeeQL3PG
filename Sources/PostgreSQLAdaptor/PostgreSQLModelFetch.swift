//
//  PostgreSQLModelFetch.swift
//  ZeeQL3Apache
//
//  Created by Helge Hess on 14/04/17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import ZeeQL

/**
 * Wraps queries which do PostgreSQL schema reflection.
 */
open class PostgreSQLModelFetch: AdaptorModelFetch {
  
  let log : ZeeQLLogger = globalZeeQLLogger
  
  public enum Error : Swift.Error {
    case GotNoSchemaVersion
    case NotImplemented
    case DidNotFindTable(String)
  }
  
  public var channel    : AdaptorChannel
  
  // TODO: not in here I think! Rather create a new model from a base-db only
  //       one.
  public let nameMapper : ModelNameMapper
  
  public init(channel: AdaptorChannel) {
    self.channel    = channel
    self.nameMapper = self.channel
  }
  
  
  
  // MARK: - Model tags
  
  public func fetchModelTag() throws -> ModelTag {
    // TBD: this is quick but rather expensive, if there is a better way,
    //      pleaze tell me :-)
    let sql = "SELECT md5(array_agg(md5((zzinfo.*)::varchar))::varchar) FROM ( "
            + PostgreSQLModelFetch.allSchemaInfoQuery
            + " ) AS zzinfo"
    
    var tagOpt : PostgreSQLModelTag? = nil
    try channel.select(sql) { ( hash : String ) in
      tagOpt = PostgreSQLModelTag(hash: hash)
    }
    guard let tag = tagOpt else { throw Error.GotNoSchemaVersion }
    return tag
  }
  
  
  // MARK: - Old-style reflection methods
  public func describeTableNames() throws -> [ String ] {
    return try channel.fetchSingleStringRows(
                         PostgreSQLModelFetch.tableNameQuery,
                         column: "table")
  }
  public func describeSequenceNames() throws -> [ String ] {
    var names = [ String ]()
    try channel.select(PostgreSQLModelFetch.seqNameQuery) {
      ( name : String ) in names.append(name)
    }
    return names
  }
  public func describeDatabaseNames() throws -> [ String ] {
    var names = [ String ]()
    try channel.select(PostgreSQLModelFetch.dbNameQuery) {
      ( name : String ) in names.append(name)
    }
    return names
  }

  public func describeModelWithTableNames(_ names: [ String ], tagged: Bool)
                throws -> Model
  {
    let didOpenTX = !channel.isTransactionInProgress
    if didOpenTX { try channel.begin() }
    
    let model : Model
    do {
      let entities = try describeEntitiesWithTableNames(names)
      
      if tagged {
        let tag = try fetchModelTag()
        model = Model(entities: entities, tag: tag)
      }
      else {
        model = Model(entities: entities)
      }
    }
    catch {
      if didOpenTX {
        try? channel.rollback() // throw the other error
      }
      throw error
    }
    
    if didOpenTX {
      try channel.rollback()
    }
    
    return model
  }

  public func describeEntitiesWithTableNames(_ tables: [ String ]) throws
                -> [ Entity ]
  {
    guard !tables.isEmpty else { return [] }
    
    let expr     = channel.expressionFactory.createExpression(nil)
    var entities = [ Entity ]()
    entities.reserveCapacity(tables.count)
    
    let tableIn = tables.map({ table in
      let schemaName = expr.sqlStringFor(schemaObjectName: table)
      return expr.sqlStringFor(string: schemaName) + "::regclass"
    }).joined(separator: ",")
    
    // TBD: use `format_type()` like below?
    //        format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
    let attributeRecords = try channel.querySQL(
      "SELECT c.relname AS table, a.attnum, a.attname AS colname, " +
             "t.typname AS exttype, a.attlen, a.attnotnull " +
        "FROM pg_class c, pg_attribute a, pg_type t " +
       "WHERE (a.attnum > 0 AND a.attrelid = c.oid AND a.atttypid = t.oid) " +
         "AND c.oid IN ( \(tableIn) ) " +
       "ORDER BY c.relname, a.attnum;"
    )
    
    // Remove 'indisprimary' to list all indexed attributes
    let pkeyRecords = try channel.querySQL(
      "SELECT pg_class.relname AS table, pg_attribute.attname AS name " +
        "FROM pg_index, pg_class, pg_attribute, pg_namespace " +
       "WHERE pg_class.oid IN ( \(tableIn) )" +
         "AND indrelid = pg_class.oid " +
         "AND nspname = 'public' " + // TBD
         "AND pg_class.relnamespace = pg_namespace.oid " +
         "AND pg_attribute.attrelid = pg_class.oid " +
         "AND pg_attribute.attnum = any(pg_index.indkey) " +
         "AND indisprimary;"
    )
    
    let autoIncrRecords = try channel.querySQL(
      "SELECT TAB.relname AS table, ATTR.attname AS name " +
      "  FROM pg_depend DEP " +
      " INNER JOIN pg_class TAB " +
      "    ON ( DEP.refobjid = TAB.oid " +
               "AND DEP.refclassid = 'pg_class'::regclass ) " +
      " INNER JOIN pg_class SEQ " +
      "    ON ( SEQ.oid = DEP.objid AND SEQ.relkind = 'S' " +
               "AND DEP.classid = 'pg_class'::regclass) " +
      " INNER JOIN pg_attribute ATTR " +
      "    ON ( ATTR.attrelid = TAB.oid AND ATTR.attnum = DEP.refobjsubid " +
               "AND DEP.deptype = 'a' ) " +
       "WHERE TAB.oid IN ( \(tableIn) );"
    )
    
    let fkeySQL = PostgreSQLModelFetch.allFKeyContraintsQuery
                + " AND tf.oid IN ( \(tableIn) );"
    let foreignKeyRecords = try channel.querySQL(fkeySQL)
    
    let recordsByTable : [ String : [ AdaptorRecord ] ] = {
      var grouped = [ String : [ AdaptorRecord ] ]()
      for record in attributeRecords {
        guard let key = record["table"] as? String else { continue } // TBD
        if case nil = grouped[key]?.append(record) {
          grouped[key] = [ record ]
        }
      }
      return grouped
    }()
    
    let pkeysByTable : [ String : [ String ] ] = {
      var grouped = [ String : [ String ] ]()
      for record in pkeyRecords {
        guard let key  = record["table"] as? String else { continue } // TBD
        guard let pkey = record["name"]  as? String else { continue }
        if case nil = grouped[key]?.append(pkey) {
          grouped[key] = [ pkey ]
        }
      }
      return grouped
    }()
    
    let autoIncrByTable : [ String : Set<String> ] = {
      var grouped = [ String : Set<String> ]()
      for record in autoIncrRecords {
        guard let key  = record["table"] as? String else { continue } // TBD
        guard let pkey = record["name"]  as? String else { continue }
        if case nil = grouped[key]?.insert(pkey) {
          grouped[key] = Set<String>([ pkey ])
        }
      }
      return grouped
    }()
    
    let fkeysByTable : [ String : [ AdaptorRecord ] ] = {
      var grouped = [ String : [ AdaptorRecord ] ]()
      for record in foreignKeyRecords {
        guard let key = record["source_table"] as? String else { continue } // TBD
        if case nil = grouped[key]?.append(record) {
          grouped[key] = [ record ]
        }
      }
      return grouped
    }()
    
    for table in tables {
      // TBD: fetch PG schema name (namespace) or add it to the method args?
      guard let columnInfos = recordsByTable[table] else {
        throw Error.DidNotFindTable(table)
      }
      let pkeyNames   = pkeysByTable[table]    ?? []
      let autoIncr    = autoIncrByTable[table]
      
      let attributes = attributesFromColumnInfos(columnInfos, autoIncr)
      
      let entity = ModelEntity(name: nameMapper.entityNameForTableName(table),
                               table: table)
      entity.attributes = attributes
      entity.primaryKeyAttributeNames =
        attributeNamesFromColumnNames(pkeyNames, attributes)
      
      
      // add relationships
      
      if let foreignKeyRecords = fkeysByTable[table] {
        let fkeysByConstraint : [ String : [ AdaptorRecord ] ] = {
          var grouped = [ String : [ AdaptorRecord ] ]()
          for record in foreignKeyRecords {
            guard let key = record["constraint_name"] as? String else { continue }
            if case nil = grouped[key]?.append(record) {
              grouped[key] = [ record ]
            }
          }
          return grouped
        }()
        
        for ( name, fkeys ) in fkeysByConstraint {
          // name is usually pretty ugly, but well. We should probably deal
          // with it.
          // sample: 'address_person_id_fkey'
          let relship = ModelRelationship(name: name, isToMany: false,
                                          source: entity, destination: nil)
          relship.constraintName = name
          for fkey in fkeys {
            // TODO: match_type, on_update(updateRule)
            
            if let constraintType = fkey["constraint_type"] as? String {
              guard constraintType.isEmpty || constraintType == "f" else {
                // Note: later we may support other constraints
                continue
              }
            }
            
            if let count = fkey["source_column_count"] as? Int, count > 1 {
              // TODO: Not too hard to add, but then pretty rare as well :-)
              log.warn("unsupported multi-column foreign-key constraint:", fkey)
              continue
            }
            
            guard let destname     = fkey["foreign_table_name"] as? String,
                  let sourceColumn = fkey["source_column"]  as? String,
                  let targetColumn = fkey["target_column"]  as? String
             else { continue }
            
            relship.destinationEntityName = destname
            let join = Join(source: sourceColumn, destination: targetColumn)
            relship.joins.append(join)
            
            if let deleteRule = fkey["on_delete"] as? String {
              switch deleteRule {
                case "r": relship.deleteRule = .deny
                case "c": relship.deleteRule = .cascade
                case "n": relship.deleteRule = .nullify
                case "d": relship.deleteRule = .applyDefault
                case "a": relship.deleteRule = .noAction
                default: log.warn("unexpected foreign-key delete rule:", fkey)
              }
            }
          }
          if !relship.joins.isEmpty {
            entity.relationships.append(relship)
          }
        }
      }
      
      entities.append(entity)
    }
    return entities
  }

  public func describeEntityWithTableName(_ table: String) throws -> Entity {
    guard let entity = try describeEntitiesWithTableNames([table]).first
     else { throw Error.DidNotFindTable(table) }
    return entity
  }

  func _fetchPGColumnsOfTable(_ table: String) throws -> [ AdaptorRecord ] {
    /* Sample result:
     *  attnum |    colname     |   exttype   | attlen | attnotnull 
     * --------+----------------+-------------+--------+------------
     *       1 | receipt_id     | int4        |      4 | t
     *       2 | object_version | int4        |      4 | t
     *       3 | db_status      | varchar     |     -1 | t
     *       4 | creation_date  | timestamptz |      8 | t
     *       5 | creator_id     | int4        |      4 | t
     *       6 | receipt_code   | varchar     |     -1 | f
     *       7 | receipt_date   | timestamptz |      8 | f
     *       8 | currency       | varchar     |     -1 | t
     *       9 | start_amount   | numeric     |     -1 | t
     *      10 | end_amount     | numeric     |     -1 | t
     *      11 | subject        | varchar     |     -1 | t
     *      12 | info           | varchar     |     -1 | f
     *      13 | account_id     | int4        |      4 | f
     */
    guard !table.isEmpty else { return [] }
    
    // TODO: escape table properly (use SQLExpression, I think SQLite does it)
    let sql = PostgreSQLModelFetch.columnBaseQuery +
              " AND c.relname='\(table)' ORDER BY attnum;";
    return try channel.querySQL(sql)
  }
  
  func _fetchPGPrimaryKeyNamesOfTable(_ table: String) throws -> [ String ] {
    guard !table.isEmpty else { return [] }
    
    let sql = PostgreSQLModelFetch.pkeyBaseQuery
                .replacingOccurrences(of: "$PKEY_TABLE_NAME$", with: table)

    var pkeys = [ String ]()
    try channel.querySQL(sql) { record in
      if let pkey = record["pkey"] as? String {
        pkeys.append(pkey)
      }
    }
    return pkeys
  }

  func attributesFromColumnInfos(_ columnInfos: [ AdaptorRecord ],
                                 _ autoincrementColumns : Set<String>? = nil)
       -> [ Attribute ]
  {
    // TODO: isAutoIncrement, precision
    
    var attributes = [ Attribute ]()
    attributes.reserveCapacity(columnInfos.count)

    // Note: in the APR variant all the values are returned as Strings ...
    //       (currently, use a proper Attribute based fetch!)
    for colinfo in columnInfos {
      guard let colname = colinfo["colname"] as? String else { continue } // Hm
      
      let exttype = (colinfo["exttype"] as? String)?.uppercased()
      
      // TODO: complete information
      let attribute =
        ModelAttribute(name: nameMapper.attributeNameForColumnName(colname),
                       column: colname,
                       externalType: exttype)
      
      if let lens = colinfo["attlen"] as? String, let len = Int(lens), len > 0 {
        attribute.width = len
      }
      
      if let nulls = colinfo["attnotnull"] as? String {
        attribute.allowsNull = nulls == "f"
      }
      else if let null = colinfo["attnotnull"] as? Bool {
        attribute.allowsNull = !null
      }
      
      if let exttype = exttype {
        attribute.valueType =
          ZeeQLTypes.valueTypeForExternalType(exttype,
                                   allowsNull: attribute.allowsNull ?? true)
      }
      
      if let ac = autoincrementColumns, ac.contains(colname) {
        attribute.isAutoIncrement = true
      }
      
      attributes.append(attribute)
    }
    
    return attributes
  }
  
  func attributeNamesFromColumnNames(_ colnames : [ String ],
                                     _ attrs    : [ Attribute ]) -> [ String ]
  {
    var attrNames = [ String ]()
    attrNames.reserveCapacity(colnames.count)
    
    for colname in colnames {
      for attr in attrs { // lame
        if colname == attr.columnName {
          attrNames.append(attr.name)
          break
        }
      }
    }
    return attrNames
  }
  
  // MARK: - Queries
  
  static let tableNameQuerySOPE =
    "SELECT relname FROM pg_class WHERE " +
    "(relkind='r') AND (relname !~ '^pg_') AND (relname !~ '^xinv[0-9]+') " +
    "ORDER BY relname"
  
  static let tableNameQuery =
    "SELECT BASE.relname AS table, BASE.relnamespace " +
      "FROM pg_class AS BASE " +
      "LEFT JOIN pg_catalog.pg_namespace N ON N.oid = BASE.relnamespace " +
     "WHERE BASE.relkind = 'r' " +
       "AND N.nspname NOT IN ('pg_catalog', 'pg_toast') " +
       "AND pg_catalog.pg_table_is_visible(BASE.oid)"
  
  /* same like above, just with a different relkind */
  static let seqNameQuery =
    "SELECT BASE.relname " +
      "FROM pg_class AS BASE " +
      "LEFT JOIN pg_catalog.pg_namespace N ON N.oid = BASE.relnamespace " +
     "WHERE BASE.relkind = 'S' " +
       "AND N.nspname NOT IN ('pg_catalog', 'pg_toast') " +
       "AND pg_catalog.pg_table_is_visible(BASE.oid)"
 
  static let dbNameQuery =
    "SELECT datname FROM pg_database ORDER BY datname"
  
  static let columnBaseQuery =
    "SELECT a.attnum, a.attname AS colname, t.typname AS exttype, " +
           "a.attlen, a.attnotnull " +
      "FROM pg_class c, pg_attribute a, pg_type t " +
     "WHERE (a.attnum > 0 AND a.attrelid = c.oid AND a.atttypid = t.oid)"
  
  static let pkeyBaseQuery =
    "SELECT attname AS pkey FROM pg_attribute " +
     "WHERE attrelid IN (" +
              "SELECT a.indexrelid FROM pg_index a, pg_class b WHERE " +
                "a.indexrelid = b.oid AND a.indisprimary AND b.relname IN (" +
                "SELECT indexname FROM pg_indexes " +
                  "WHERE tablename = '$PKEY_TABLE_NAME$'" +
              ")" +
            ")"

  // TBD: is this good enough? :-)
  // TODO: add foreign keys
  static let allSchemaInfoQuery =
    "SELECT c.relname AS table, c.relnamespace, " +
           "a.attnum, a.attname AS colname, t.typname AS exttype, " +
           "a.attlen, a.attnotnull " +
      "FROM pg_class                     c " +
     "INNER JOIN pg_attribute            a ON ( a.attrelid = c.oid ) " +
     "INNER JOIN pg_type                 t ON ( a.atttypid = t.oid ) " +
      "LEFT JOIN pg_catalog.pg_namespace N ON ( N.oid = c.relnamespace ) " +
     "WHERE a.attnum > 0 " +
       "AND c.relkind = 'r' " +
       "AND N.nspname NOT IN ('pg_catalog', 'pg_toast') " +
       "AND pg_catalog.pg_table_is_visible(c.oid)"
  
  // TODO: fetch all assignments of a constraint. This fetches the count, but
  //       only the first foreign key
  static let allFKeyContraintsQuery =
    "SELECT c.conname AS constraint_name, " +
            "QUOTE_IDENT(tf.relname)   AS source_table, "  +
            "QUOTE_IDENT(tfa.attname)  AS source_column, " +
            "array_length(c.conkey, 1) AS source_column_count, " +
            "QUOTE_IDENT(tt.relname)   AS foreign_table_name, "  +
            "QUOTE_IDENT(tta.attname)  AS target_column, " +
            "c.contype  AS constraint_type, " +
            "confupdtype AS on_update, " +
            "confdeltype AS on_delete, " +
            "confmatchtype::text  AS match_type " +
      "FROM pg_catalog.pg_constraint AS c " +
     "INNER JOIN pg_class     AS tf  ON tf.oid  = c.conrelid " +
     "INNER JOIN pg_attribute AS tfa " +
        "ON ( tfa.attrelid = tf.oid AND tfa.attnum = c.conkey[1] ) "  +
     "INNER JOIN pg_class     AS tt "  +
        "ON tt.oid  = c.confrelid "    +
     "INNER JOIN pg_attribute AS tta " +
        "ON ( tta.attrelid = tt.oid AND tta.attnum = c.confkey[1] ) " +
     "WHERE c.contype = 'f'"
}

public struct PostgreSQLModelTag : ModelTag, Equatable {
  let hash : String
  
  public func isEqual(to object: Any?) -> Bool {
    guard let object = object else { return false }
    guard let other = object as? PostgreSQLModelTag else { return false }
    return self == other
  }
  public static func ==(lhs: PostgreSQLModelTag, rhs: PostgreSQLModelTag)
                     -> Bool
  {
    return lhs.hash == rhs.hash
  }
}
