//
//  CopyEscapeTests.swift
//  MIODBPostgreSQLTests
//
//  COPY text-format escaping, no server needed.
//
//  Regression covered: a CRLF pair is a single Swift Character, so a
//  Character-level scan for "\r" / "\n" never matched it and the literal
//  carriage return went into the COPY stream — Postgres rejected the row
//  with 22P04 "literal carriage return found in data" (account_annotation,
//  DLHookServer, 2026-08-07).
//

import XCTest
@testable import MIODBPostgreSQL

final class CopyEscapeTests: XCTestCase {

    private func escape(_ s: String) -> String {
        let db = MIODBPostgreSQL(host: "", user: "", password: "", database: "" )
        var out = ""
        db.copyEscape(s, into: &out)
        return out
    }

    func testPlainStringPassesThrough() {
        XCTAssertEqual(escape("hello world"), "hello world")
    }

    func testSpecialCharactersAreEscaped() {
        XCTAssertEqual(escape("a\tb"), "a\\tb")
        XCTAssertEqual(escape("a\nb"), "a\\nb")
        XCTAssertEqual(escape("a\rb"), "a\\rb")
        XCTAssertEqual(escape("a\\b"), "a\\\\b")
    }

    func testCRLFGraphemeClusterIsEscaped() {
        // The 22P04 regression: "\r\n" is ONE Character in Swift, invisible
        // to a Character-level contains check
        XCTAssertEqual(escape("line one\r\nline two"), "line one\\r\\nline two")
    }

    func testMixedLineEndings() {
        XCTAssertEqual(escape("a\r\nb\rc\nd"), "a\\r\\nb\\rc\\nd")
    }

    func testUnicodeSurvivesScalarIteration() {
        // Multi-scalar graphemes (emoji with modifiers, combining accents)
        // must round-trip untouched when re-assembled from scalars
        let s = "café 👨‍👩‍👧‍👦 mañana\r\nfin"
        XCTAssertEqual(escape(s), "café 👨‍👩‍👧‍👦 mañana\\r\\nfin")
    }

    // Legacy Linux test discovery (LinuxMain/XCTestManifests) only runs what
    // the manifest lists — keep this in sync or the suite silently vanishes
    static var allTests = [
        ("testPlainStringPassesThrough", testPlainStringPassesThrough),
        ("testSpecialCharactersAreEscaped", testSpecialCharactersAreEscaped),
        ("testCRLFGraphemeClusterIsEscaped", testCRLFGraphemeClusterIsEscaped),
        ("testMixedLineEndings", testMixedLineEndings),
        ("testUnicodeSurvivesScalarIteration", testUnicodeSurvivesScalarIteration),
    ]
}
