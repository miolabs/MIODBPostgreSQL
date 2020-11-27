//
//  File.swift
//  
//
//  Created by David Trallero on 26/11/2020.
//

import Foundation
import MIODB

open class MDBPostgreConnection : MDBConnection {    
    public func craete ( ) throws -> MIODB {
        return MIODBPostgreSQL( connection: self )
    }
}
