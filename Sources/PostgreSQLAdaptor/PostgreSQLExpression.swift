//
//  PostgreSQLAdaptor.swift
//  ZeeQL
//
//  Created by Helge Hess on 03/03/17.
//  Copyright © 2017 ZeeZide GmbH. All rights reserved.
//

import ZeeQL

// MARK: - Expressions

open class PostgreSQLExpressionFactory: SQLExpressionFactory {
  
  static let shared = PostgreSQLExpressionFactory()
  
  override open func createExpression(_ entity: Entity?) -> SQLExpression {
    return PostgreSQLExpression(entity: entity)
  }
}

open class PostgreSQLExpression: SQLExpression {
  
  var bindCounter : Int = 0

  override open func bindVariableDictionary(for attribute: Attribute?,
                                            value: Any?)
                     -> BindVariable
  {
    var bind = super.bindVariableDictionary(for: attribute, value: value)
    bindCounter += 1
    bind.placeholder = "$\(bindCounter)"
    return bind
  }

  override open var sqlStringForCaseInsensitiveLike : String? {
    return "ILIKE"
  }

  // MARK: - Insert w/ returning

  open func prepareInsertReturningExpressionWithRow
              (_ row: AdaptorRow, attributes attrs: [Attribute]?)
  {
    // Note: we need the entity for the table name ...
    guard entity != nil else { return }
    
    // prepareSelectExpressionWithAttributes(attrs, lock, fs)
    
    useAliases = false
    
    /* prepare columns to select */
    
    let columns : String
    
    if let attrs = attrs {
      if !attrs.isEmpty {
        listString.removeAll()
        for attr in attrs {
          self.addSelectListAttribute(attr)
        }
        columns = listString
        listString.removeAll()
      }
      else {
        columns = "*"
      }
    }
    else {
      columns = "*"
    }
    
    /* create insert */
    
    prepareInsertExpressionWithRow(row)
    
    /* add returning */
    
    statement += " RETURNING " + columns
  }
}
