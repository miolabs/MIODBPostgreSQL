//
//  MIODBPostgreSQL+Copy.swift
//  MIODBPostgreSQL
//
//  Created by Javier Segura Perez on 12/07/2026.
//
//  COPY FROM STDIN fast path for large multi-row inserts. PostgreSQL loads
//  COPY data significantly faster than a multi-row INSERT because the rows
//  stream as data instead of being parsed as SQL, and the client never
//  materializes the giant statement string.
//

import Foundation
import MIOCore
import MIODB
@_implementationOnly import CLibPQ
import MIOCoreLogger

extension MIODBPostgreSQL
{
    /// Row-count threshold above which a MULTI_INSERT is executed with COPY.
    /// Below it a multi-VALUES INSERT is as fast or faster (COPY has a fixed
    /// mode-switch cost). Set MDB_POSTGRESQL_COPY_THRESHOLD to tune, or to
    /// 0/negative to disable the COPY path entirely.
    static let copyThreshold: Int = {
        if let v = MCEnvironmentVar( "MDB_POSTGRESQL_COPY_THRESHOLD" ), let n = Int( v ) { return n }
        return 500
    }()

    /// COPY handles plain typed values. SQL fragments (MDBValue(raw:)) and
    /// array literals cannot be represented as COPY data — those queries fall
    /// back to the regular INSERT path.
    func copyCanEncode ( _ query: MDBQuery ) -> Bool {
        for row in query.multiValues {
            for (_, v) in row {
                switch v.storage {
                case .raw, .array: return false
                default: break
                }
            }
        }
        return true
    }

    /// Escapes a string for COPY text format: backslash, tab, newline and
    /// carriage return are the only characters with meaning.
    private func copyEscape ( _ s: String, into out: inout String ) {
        guard s.contains( where: { $0 == "\\" || $0 == "\t" || $0 == "\n" || $0 == "\r" } ) else {
            out += s
            return
        }
        for ch in s {
            switch ch {
            case "\\": out += "\\\\"
            case "\t": out += "\\t"
            case "\n": out += "\\n"
            case "\r": out += "\\r"
            default:   out.append( ch )
            }
        }
    }

    private func copyEncode ( _ storage: MDBValueStorage, into out: inout String ) {
        switch storage {
        case .null:                   out += "\\N"
        case .bool( let b ):          out += b ? "t" : "f"
        case .int( let i ):           out += String( i )
        case .float( let f ):         out += String( f )
        case .double( let d ):        out += String( d )
        case .decimal( let d ):       out += NSDecimalNumber( decimal: d ).stringValue
        case .string( let s ):        copyEscape( s, into: &out )
        case .partialString( let s ): copyEscape( "%" + s + "%", into: &out )
        case .uuid( let u ):          out += u.uuidString.uppercased()
        case .date( let d ):          out += MDBSQLTimestampString( d )
        case .json( let j ):          copyEscape( j, into: &out )
        case .raw, .array:            out += "\\N" // unreachable: copyCanEncode filters these
        }
    }

    private func putCopyData ( _ buffer: String ) throws {
        let ok = buffer.withCString { cstr in
            PQputCopyData( _connection, cstr, Int32( strlen( cstr ) ) )
        }
        if ok != 1 {
            let msg = String( cString: PQerrorMessage( _connection ) )
            throw MIODBPostgreSQLError.fatalError( "-4", "COPY data send failed: \(msg)" )
        }
    }

    /// Drains every pending PGresult after a COPY, returning the first
    /// COMMAND_OK result (which the caller wraps into the result set) or
    /// throwing with the server error. The connection MUST be fully drained
    /// even on failure or it is left unusable.
    private func drainCopyResults ( ) throws -> OpaquePointer {
        var finalRes: OpaquePointer? = nil
        var errorMsg: String? = nil
        var errorCode = "0"

        while let r = PQgetResult( _connection ) {
            let status = PQresultStatus( r )
            if status == PGRES_COMMAND_OK && finalRes == nil && errorMsg == nil {
                finalRes = r
                continue
            }
            if status == PGRES_FATAL_ERROR && errorMsg == nil {
                errorMsg = String( cString: PQresultErrorMessage( r ) )
                let ec = PQresultErrorField( r, 67 ) // PG_DIAG_SQLSTATE
                errorCode = ec != nil ? String( cString: ec! ) : "0"
            }
            PQclear( r )
        }

        if let msg = errorMsg {
            if finalRes != nil { PQclear( finalRes ) }
            throw MIODBPostgreSQLError.fatalError( errorCode, (scheme != nil ? "\(scheme!): " : "") + msg )
        }
        guard let res = finalRes else {
            throw MIODBPostgreSQLError.fatalError( "-4", "COPY finished without a result" )
        }
        return res
    }

    /// Executes a MULTI_INSERT as COPY table (cols) FROM STDIN, streaming the
    /// rows in ~64KB chunks. Caller guarantees copyCanEncode passed and the
    /// query has no RETURNING clause.
    func copyInsert ( _ query: MDBQuery ) throws -> MDBPostgreSQLResultSet {
        queryWillExecute()
        defer { queryDidExecute() }

        if PQstatus( _connection ) != CONNECTION_OK {
            Log.error( "ID: \(identifier). Postgres connection was lost, re-connecting and crossing fingers" )
            disconnect()
            usleep( 500000 ) // 0.5 seconds
            try connect()
        }

        let sorted = query.sortedValues( query.multiValues.count > 0 ? query.multiValues[ 0 ] : [:] )
        let columns = sorted.map{ "\"\($0.key)\"" }.joined( separator: "," )
        let copySQL = "COPY " + MDBValue( fromTable: query.table ).value + " (" + columns + ") FROM STDIN"

        Log.trace( "ID: \(identifier). QUERY: \(copySQL) -- \(query.multiValues.count) rows" )

        let start = copySQL.withCString { PQexec( _connection, $0 ) }
        guard let start = start, PQresultStatus( start ) == PGRES_COPY_IN else {
            let msg = start != nil ? String( cString: PQresultErrorMessage( start ) )
                                   : String( cString: PQerrorMessage( _connection ) )
            if start != nil { PQclear( start ) }
            throw MIODBPostgreSQLError.fatalError( "-4", (scheme != nil ? "\(scheme!): " : "") + msg + "\n" + copySQL )
        }
        PQclear( start )

        do {
            var buffer = String()
            buffer.reserveCapacity( 96 * 1024 )

            for row in query.multiValues {
                var first = true
                for col in sorted {
                    if !first { buffer += "\t" }
                    first = false
                    if let v = row[ col.key ] {
                        copyEncode( v.storage, into: &buffer )
                    } else {
                        // Same gap-fill as MDBQuery.multiValuesRaw: rows missing a
                        // _relation column get '' — anything else gets NULL
                        buffer += col.key.starts( with: "_relation" ) ? "" : "\\N"
                    }
                }
                buffer += "\n"

                if buffer.utf8.count >= 64 * 1024 {
                    try putCopyData( buffer )
                    buffer.removeAll( keepingCapacity: true )
                }
            }
            if !buffer.isEmpty { try putCopyData( buffer ) }
        }
        catch {
            // Abort the COPY and drain, or the connection stays in copy-in state
            _ = "client error".withCString { PQputCopyEnd( _connection, $0 ) }
            _ = try? drainCopyResults()
            throw error
        }

        if PQputCopyEnd( _connection, nil ) != 1 {
            let msg = String( cString: PQerrorMessage( _connection ) )
            _ = try? drainCopyResults()
            throw MIODBPostgreSQLError.fatalError( "-4", "COPY end failed: \(msg)" )
        }

        let res = try drainCopyResults()

        // The result set owns the PGresult from here and clears it on deinit.
        return MDBPostgreSQLResultSet( result: res, db: self )
    }
}
