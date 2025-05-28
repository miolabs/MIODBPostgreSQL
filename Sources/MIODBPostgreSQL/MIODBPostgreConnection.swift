//
//  MIODBPostgreConnection.swift
//  MIODBPostgreSQL
//
//  Created by David Trallero on 26/11/2020.
//

import Foundation
import MIODB


open class MDBPostgreConnection : MDBConnection
{
    open override func create ( _ to_db: String?, identifier: String? = nil, label: String? = nil, delegate: MDBDelegate? = nil ) throws -> MIODB {
        let db = MIODBPostgreSQL( connection: self )
        db.delegate = delegate
        if let id = identifier { db.identifier = id }
        if let lbl = label { db.label = lbl }
        try db.connect( to_db )
        return db
    }
}
