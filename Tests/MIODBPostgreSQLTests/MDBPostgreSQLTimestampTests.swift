//
//  MDBPostgreSQLTimestampTests.swift
//  MIODBPostgreSQL
//
//  Created by Javier Segura Perez on 08/07/2026.
//

import XCTest
@testable import MIODBPostgreSQL

final class MDBPostgreSQLTimestampTests: XCTestCase
{
    func parse ( _ str: String ) -> Date? {
        return str.withCString { MDBPostgreSQLParseTimestamp( $0 ) }
    }

    func iso ( _ str: String ) -> Date {
        let f = ISO8601DateFormatter()
        return f.date( from: str )!
    }

    func testWholeSecondsUTC () {
        XCTAssertEqual( parse( "2026-07-08 17:23:40" ), iso( "2026-07-08T17:23:40Z" ) )
        XCTAssertEqual( parse( "2026-07-08 17:23:40+00" ), iso( "2026-07-08T17:23:40Z" ) )
        XCTAssertEqual( parse( "1999-12-31 23:59:59" ), iso( "1999-12-31T23:59:59Z" ) )
    }

    func testFractionalSeconds () {
        let base = iso( "2026-07-08T17:23:40Z" ).timeIntervalSince1970

        XCTAssertEqual( parse( "2026-07-08 17:23:40.5" )!.timeIntervalSince1970, base + 0.5, accuracy: 1e-6 )
        XCTAssertEqual( parse( "2026-07-08 17:23:40.123" )!.timeIntervalSince1970, base + 0.123, accuracy: 1e-6 )
        XCTAssertEqual( parse( "2026-07-08 17:23:40.123456" )!.timeIntervalSince1970, base + 0.123456, accuracy: 1e-6 )
        XCTAssertEqual( parse( "2026-07-08 17:23:40.000001+00" )!.timeIntervalSince1970, base + 0.000001, accuracy: 1e-6 )
    }

    func testTimeZoneOffsets () {
        XCTAssertEqual( parse( "2026-07-08 17:23:40+02" ), iso( "2026-07-08T17:23:40+02:00" ) )
        XCTAssertEqual( parse( "2026-07-08 17:23:40-05" ), iso( "2026-07-08T17:23:40-05:00" ) )
        XCTAssertEqual( parse( "2026-07-08 17:23:40+05:30" ), iso( "2026-07-08T17:23:40+05:30" ) )

        // Offsets with seconds exist in Postgres but not in ISO8601DateFormatter
        let base = iso( "2026-07-08T17:23:40Z" ).timeIntervalSince1970
        let offset = Double( 5 * 3600 + 30 * 60 + 15 )
        XCTAssertEqual( parse( "2026-07-08 17:23:40+05:30:15" )!.timeIntervalSince1970, base - offset, accuracy: 1e-6 )

        // Fraction and offset combined
        XCTAssertEqual( parse( "2026-07-08 17:23:40.25+02" )!.timeIntervalSince1970,
                        iso( "2026-07-08T17:23:40+02:00" ).timeIntervalSince1970 + 0.25, accuracy: 1e-6 )
    }

    func testDateOnly () {
        XCTAssertEqual( parse( "2026-07-08" ), iso( "2026-07-08T00:00:00Z" ) )
        XCTAssertEqual( parse( "2024-02-29" ), iso( "2024-02-29T00:00:00Z" ) ) // leap day
        XCTAssertEqual( parse( "1970-01-01" ), Date( timeIntervalSince1970: 0 ) )
    }

    func testHistoricAndPreEpoch () {
        XCTAssertEqual( parse( "1969-12-31 23:59:59" ), iso( "1969-12-31T23:59:59Z" ) )
        XCTAssertEqual( parse( "1900-01-01 00:00:00" ), iso( "1900-01-01T00:00:00Z" ) )

        // Postgres uses the proleptic Gregorian calendar for all dates;
        // ISO8601DateFormatter switches to Julian before 1582, so the
        // reference here is the proleptic epoch value, not the formatter.
        XCTAssertEqual( parse( "0001-01-01 00:00:00" ), Date( timeIntervalSince1970: -62_135_596_800 ) )
    }

    func testNonISOInputFallsBack () {
        XCTAssertNil( parse( "infinity" ) )
        XCTAssertNil( parse( "-infinity" ) )
        XCTAssertNil( parse( "0042-07-08 17:23:40 BC" ) )      // trailing era
        XCTAssertNil( parse( "2026-07-08T17:23:40" ) )         // ISO 'T' separator is not PG wire format
        XCTAssertNil( parse( "2026-13-08 17:23:40" ) )         // invalid month
        XCTAssertNil( parse( "2026-07-32" ) )                  // invalid day
        XCTAssertNil( parse( "2026-07-08 17:23" ) )            // missing seconds
        XCTAssertNil( parse( "2026-07-08 17:23:40." ) )        // empty fraction
        XCTAssertNil( parse( "2026-07-08 17:23:40+2" ) )       // one-digit offset
        XCTAssertNil( parse( "2026-07-08 17:23:40 extra" ) )   // trailing junk
        XCTAssertNil( parse( "" ) )
    }

    /// The fast path must agree with the formatter path it replaces.
    func testAgreesWithLegacyFormatter () {
        let legacy = ISO8601DateFormatter()
        legacy.formatOptions = [.withInternetDateTime, .withFractionalSeconds]

        let samples = [
            ( "2026-07-08 17:23:40.123", "2026-07-08T17:23:40.123+00:00" ),
            ( "2026-01-01 00:00:00.5+01", "2026-01-01T00:00:00.500+01:00" ),
            ( "1985-11-05 08:15:30.999-06", "1985-11-05T08:15:30.999-06:00" ),
        ]

        for (pg, isoStr) in samples {
            let expected = legacy.date( from: isoStr )!
            XCTAssertEqual( parse( pg )!.timeIntervalSince1970, expected.timeIntervalSince1970, accuracy: 1e-6, "for \(pg)" )
        }
    }
}
