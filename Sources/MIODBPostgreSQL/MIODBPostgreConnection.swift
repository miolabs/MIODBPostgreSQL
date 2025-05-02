//
//  File.swift
//  
//
//  Created by David Trallero on 26/11/2020.
//

import Foundation
import MIODB


open class MDBPostgreConnection : MDBConnection
{
    open override func create ( _ to_db: String?, id: Int = -1 ) throws -> MIODB {
        let db = MIODBPostgreSQL( connection: self )
        try db.connect( to_db, id: id )
        return db
    }
}
