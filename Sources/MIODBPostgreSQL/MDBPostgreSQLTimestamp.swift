//
//  MDBPostgreSQLTimestamp.swift
//  MIODBPostgreSQL
//
//  Created by Javier Segura Perez on 08/07/2026.
//

import Foundation

/// Parses Postgres ISO-datestyle date/timestamp text straight off the libpq
/// buffer: `YYYY-MM-DD[ HH:MM:SS[.ffffff]][±HH[:MM[:SS]]]`.
///
/// Pure integer math — no String allocation, no formatters, thread-safe.
/// A timestamp without offset is taken as UTC, matching the server wire
/// format. Returns nil for anything else (BC dates, infinity, non-ISO
/// datestyle) so callers can fall back to the formatter-based path.
func MDBPostgreSQLParseTimestamp ( _ p: UnsafePointer<Int8> ) -> Date?
{
    var i = 0

    func digits ( _ n: Int ) -> Int? {
        var v = 0
        for _ in 0..<n {
            let c = p[i]
            if c < 48 || c > 57 { return nil } // '0'...'9'
            v = v * 10 + Int(c - 48)
            i += 1
        }
        return v
    }

    guard let y = digits(4), p[i] == 45 else { return nil } // '-'
    i += 1
    guard let mo = digits(2), p[i] == 45 else { return nil } // '-'
    i += 1
    guard let d = digits(2) else { return nil }
    guard mo >= 1, mo <= 12, d >= 1, d <= 31 else { return nil }

    // Days since 1970-01-01 in the proleptic Gregorian calendar
    // (Howard Hinnant's days_from_civil).
    let yy  = mo <= 2 ? y - 1 : y
    let era = (yy >= 0 ? yy : yy - 399) / 400
    let yoe = yy - era * 400
    let doy = (153 * (mo > 2 ? mo - 3 : mo + 9) + 2) / 5 + d - 1
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy
    let days = era * 146097 + doe - 719468

    var seconds = Double(days) * 86400.0

    // Date-only column: midnight UTC.
    if p[i] == 0 { return Date(timeIntervalSince1970: seconds) }

    guard p[i] == 32 else { return nil } // ' '
    i += 1
    guard let h = digits(2), p[i] == 58 else { return nil } // ':'
    i += 1
    guard let mi = digits(2), p[i] == 58 else { return nil } // ':'
    i += 1
    guard let s = digits(2) else { return nil }
    guard h < 24, mi < 60, s <= 60 else { return nil }

    seconds += Double(h * 3600 + mi * 60 + s)

    if p[i] == 46 { // '.'
        i += 1
        var frac = 0.0, scale = 0.1
        let start = i
        while p[i] >= 48 && p[i] <= 57 {
            frac += Double(p[i] - 48) * scale
            scale *= 0.1
            i += 1
        }
        if i == start { return nil }
        seconds += frac
    }

    if p[i] == 43 || p[i] == 45 { // '+' / '-'
        let negative = p[i] == 45
        i += 1
        guard let tzh = digits(2) else { return nil }
        var offset = tzh * 3600
        if p[i] == 58 { // ':'
            i += 1
            guard let tzm = digits(2) else { return nil }
            offset += tzm * 60
            if p[i] == 58 { // ':'
                i += 1
                guard let tzs = digits(2) else { return nil }
                offset += tzs
            }
        }
        seconds += negative ? Double(offset) : -Double(offset)
    }

    // Trailing text (" BC", junk) means this is not a plain ISO value.
    guard p[i] == 0 else { return nil }

    return Date(timeIntervalSince1970: seconds)
}
