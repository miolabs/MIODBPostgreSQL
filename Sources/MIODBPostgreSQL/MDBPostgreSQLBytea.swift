//
//  MDBPostgreSQLBytea.swift
//  MIODBPostgreSQL
//
//  A bytea column arrives in text mode as PostgreSQL's hex output format:
//  "\x" followed by two hex digits per byte (bytea_output = 'hex', the default
//  since 9.0). The legacy escape format is not produced by any supported server
//  configuration we run and is rejected rather than half-parsed.
//

import Foundation

/// Decodes PostgreSQL's hex bytea text form into bytes. Returns nil for
/// anything that is not `\x` followed by an even number of hex digits.
func MDBPostgreSQLDecodeBytea ( _ text: String ) -> Data? {
    guard text.hasPrefix( "\\x" ) else { return nil }
    let hex = text.dropFirst( 2 )
    guard hex.count % 2 == 0 else { return nil }

    var data = Data( capacity: hex.count / 2 )
    var index = hex.startIndex
    while index < hex.endIndex {
        let next = hex.index( index, offsetBy: 2 )
        guard let byte = UInt8( hex[ index..<next ], radix: 16 ) else { return nil }
        data.append( byte )
        index = next
    }
    return data
}
