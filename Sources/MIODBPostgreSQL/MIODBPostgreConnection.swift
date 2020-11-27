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
    public override func create ( ) throws -> MIODB {
        let db = MIODBPostgreSQL( connection: self )
        try db.connect()
        return db
    }
}
