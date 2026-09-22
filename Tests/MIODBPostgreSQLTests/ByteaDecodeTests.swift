//
//  ByteaDecodeTests.swift
//  MIODBPostgreSQLTests
//
//  bytea columns come back in PostgreSQL's hex text format and must turn into
//  the bytes MIOCoreData stores for a Binary attribute.
//

import XCTest
import MIODB
@testable import MIODBPostgreSQL

final class ByteaDecodeTests: XCTestCase {

    func testHexFormatDecodesToBytes() {
        XCTAssertEqual(MDBPostgreSQLDecodeBytea("\\x01abff"), Data([0x01, 0xAB, 0xFF]))
        XCTAssertEqual(MDBPostgreSQLDecodeBytea("\\x01ABFF"), Data([0x01, 0xAB, 0xFF]), "upper-case digits are valid too")
        XCTAssertEqual(MDBPostgreSQLDecodeBytea("\\x"), Data(), "an empty bytea is empty data, not nil")
    }

    func testRoundTripWithTheRenderedLiteralForm() {
        // What MIODB renders on the way in (decode('01abff','hex')) is what comes back as \x01abff.
        let bytes = Data((0...255).map { UInt8($0) })
        let hex = bytes.map { String(format: "%02x", $0) }.joined()
        XCTAssertEqual(MDBPostgreSQLDecodeBytea("\\x" + hex), bytes)
    }

    func testMalformedTextIsRejected() {
        XCTAssertNil(MDBPostgreSQLDecodeBytea("01abff"), "missing \\x prefix")
        XCTAssertNil(MDBPostgreSQLDecodeBytea("\\x01a"), "odd digit count")
        XCTAssertNil(MDBPostgreSQLDecodeBytea("\\x01zz"), "non-hex digit")
        XCTAssertNil(MDBPostgreSQLDecodeBytea("AQID"), "base64 is the wire form, never the column form")
    }

    func testCopyEncodesBytesAsEscapedHex() {
        // COPY text format: the bytea hex input "\x01ab" with its backslash escaped.
        let db = MIODBPostgreSQL(host: "", user: "", password: "", database: "")
        var out = ""
        db.copyEncode(.bytes(Data([0x01, 0xAB])), into: &out)
        XCTAssertEqual(out, "\\\\x01ab")
        out = ""
        db.copyEncode(.bytes(Data()), into: &out)
        XCTAssertEqual(out, "\\\\x")
    }
}
