//
//  MDBPostgreSQLResultSet.swift
//  MIODBPostgreSQL
//
//  Created by Javier Segura Perez on 07/07/2026.
//  Copyright © 2026 Javier Segura Perez. All rights reserved.
//

import Foundation
import MIODB
@_implementationOnly import CLibPQ

/// PostgreSQL backend of `MDBResultSet`, a lazy view over a libpq `PGresult`.
///
/// The result set takes ownership of the server response and keeps the raw
/// values untouched. Cell values are converted to Swift types only when they
/// are accessed, so queries whose columns are partially consumed never pay
/// for the conversion of the rest.
///
/// The underlying `PGresult` is independent of the connection socket: the
/// connection can be closed, reused or reconnected while a result set is
/// still alive. The result set does keep the `MIODBPostgreSQL` instance
/// alive, because value conversion (the `convert` method and the
/// `queryDelegate` custom conversions) belongs to it.
public final class MDBPostgreSQLResultSet : MDBResultSet
{
    let column_types: [Oid]
    let db: MIODBPostgreSQL

    private var _result: OpaquePointer? // PGresult*, owned. PQclear on deinit.

    init ( result: OpaquePointer, db: MIODBPostgreSQL )
    {
        _result = result
        self.db = db

        let col_count = Int( PQnfields( result ) )
        var cols = [String]() ; cols.reserveCapacity( col_count )
        var types = [Oid]() ; types.reserveCapacity( col_count )

        for col in 0..<Int32( col_count ) {
            cols.append( String( cString: PQfname( result, col ) ) )
            types.append( PQftype( result, col ) )
        }

        column_types = types

        super.init( columns: cols,
                    rowCount: Int( PQntuples( result ) ),
                    affectedRowCount: strtol( PQcmdTuples( result ), nil, 10 ) )
    }

    deinit {
        if let r = _result { PQclear( r ) }
    }

    // MARK: - Cell primitives

    public override func isNull ( row: Int, col: Int ) -> Bool {
        return PQgetisnull( _result, Int32( row ), Int32( col ) ) == 1
    }

    public override func rawValue ( row: Int, col: Int ) -> String? {
        let r = Int32( row ), c = Int32( col )
        guard let res = _result, PQgetisnull( res, r, c ) == 0 else { return nil }
        let len = Int( PQgetlength( res, r, c ) )
        guard let value = PQgetvalue( res, r, c ) else { return nil }
        return value.withMemoryRebound( to: UInt8.self, capacity: len ) {
            String( decoding: UnsafeBufferPointer( start: $0, count: len ), as: UTF8.self )
        }
    }

    /// Throws only for values that fail JSON parsing (json/jsonb columns).
    public override func value ( row: Int, col: Int ) throws -> Any? {
        let r = Int32( row ), c = Int32( col )
        guard let res = _result else { return NSNull() }
        if PQgetisnull( res, r, c ) == 1 { return NSNull() }

        guard let raw = PQgetvalue( res, r, c ) else { return NSNull() }

        if let delegate = db.queryDelegate {
            let (converted, v) = delegate.customValueConvertion( field: columns[ col ], value: raw )
            if converted { return v }
        }

        return try db.convert( value: raw, withType: column_types[ col ] )
    }

    /// Integer fast path: parses the cell straight from the C buffer without
    /// allocating a String or boxing into Any.
    public override func intValue ( row: Int, col: Int ) -> Int? {
        let r = Int32( row ), c = Int32( col )
        guard let res = _result, PQgetisnull( res, r, c ) == 0 else { return nil }
        guard let value = PQgetvalue( res, r, c ) else { return nil }
        var end: UnsafeMutablePointer<Int8>? = nil
        let n = strtoll( value, &end, 10 )
        return end == UnsafeMutablePointer( mutating: value ) ? nil : Int( n )
    }

    public override func boolValue ( row: Int, col: Int ) -> Bool? {
        let r = Int32( row ), c = Int32( col )
        guard let res = _result, PQgetisnull( res, r, c ) == 0 else { return nil }
        guard let value = PQgetvalue( res, r, c ) else { return nil }
        switch value[ 0 ] {
        case 116: return true   // 't'
        case 102: return false  // 'f'
        default : return nil
        }
    }
}
