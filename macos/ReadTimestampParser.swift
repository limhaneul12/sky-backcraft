import Foundation

/// Owns the ISO-8601 formatters for one synchronous read/decode invocation.
/// Callers must create a fresh parser for each invocation and pass it through row decoders.
final class ReadTimestampParser {
    private let ordinary: ISO8601DateFormatter
    private let fractional: ISO8601DateFormatter

    private static let pow10: [Double] = [1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9]
    private static let cumulativeDays: [Int] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]

    init() {
        ordinary = ISO8601DateFormatter()
        let fractional = ISO8601DateFormatter()
        fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        self.fractional = fractional
    }

    private enum CanonicalParseResult {
        case parsed(Date)
        case invalid
        case fallback
    }

    func ordinaryThenFractional(_ value: String) -> Date? {
        switch Self.parseCanonicalUTC(value) {
        case .parsed(let date): return date
        case .invalid: return nil
        case .fallback: return ordinary.date(from: value) ?? fractional.date(from: value)
        }
    }

    func fractionalThenOrdinary(_ value: String) -> Date? {
        switch Self.parseCanonicalUTC(value) {
        case .parsed(let date): return date
        case .invalid: return nil
        case .fallback: return fractional.date(from: value) ?? ordinary.date(from: value)
        }
    }

    /// Fast path for canonical `YYYY-MM-DDTHH:MM:SS(.f{1,3})Z` UTC strings.
    /// Non-canonical shapes (offsets, >3 fractional digits) return `.fallback` so
    /// `ISO8601DateFormatter` handles them; impossible calendar fields in canonical
    /// UTC strings return `.invalid` directly.
    private static func parseCanonicalUTC(_ value: String) -> CanonicalParseResult {
        var copy = value
        return copy.withUTF8 { parseCanonicalUTC($0) }
    }

    private static func parseCanonicalUTC(_ bytes: UnsafeBufferPointer<UInt8>) -> CanonicalParseResult {
        let count = bytes.count
        guard count >= 20, count <= 24,
              bytes[count - 1] == 0x5A,
              bytes[4] == 0x2D, bytes[7] == 0x2D, bytes[10] == 0x54,
              bytes[13] == 0x3A, bytes[16] == 0x3A else { return .fallback }
        var fractionDigits = 0
        if count > 20 {
            guard bytes[19] == 0x2E else { return .fallback }
            fractionDigits = count - 21
            guard fractionDigits >= 1, fractionDigits <= 3 else { return .fallback }
        }
        func parseDigits(_ start: Int, _ length: Int) -> Int? {
            var acc = 0
            for index in start..<(start + length) {
                let digit = Int(bytes[index]) &- 48
                guard digit >= 0, digit <= 9 else { return nil }
                acc = acc * 10 + digit
            }
            return acc
        }
        guard let year = parseDigits(0, 4),
              let month = parseDigits(5, 2),
              let day = parseDigits(8, 2),
              let hour = parseDigits(11, 2),
              let minute = parseDigits(14, 2),
              let second = parseDigits(17, 2) else { return .fallback }
        var fraction = 0.0
        if fractionDigits > 0 {
            guard let rawFraction = parseDigits(20, fractionDigits) else { return .fallback }
            fraction = Double(rawFraction) / pow10[fractionDigits]
        }
        guard year >= 1, month >= 1, month <= 12, day >= 1,
              hour <= 23, minute <= 59, second <= 59 else { return .invalid }
        let isLeapYear = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
        let daysInMonth: Int
        switch month {
        case 2: daysInMonth = isLeapYear ? 29 : 28
        case 4, 6, 9, 11: daysInMonth = 30
        default: daysInMonth = 31
        }
        guard day <= daysInMonth else { return .invalid }
        let prevYear = year - 1
        let daysBeforeYear = prevYear * 365 + prevYear / 4 - prevYear / 100 + prevYear / 400
        let dayOfYear = cumulativeDays[month - 1] + (month > 2 && isLeapYear ? 1 : 0) + day - 1
        let unixDays = daysBeforeYear + dayOfYear - 719_162
        let seconds = unixDays * 86_400 + hour * 3_600 + minute * 60 + second
        return .parsed(Date(timeIntervalSince1970: Double(seconds) + fraction))
    }
}

/// Zero-allocation UTF-8 decimal validator and POSIX parser shared across native readers.
enum StrictDecimal {
    static let posixLocale = Locale(identifier: "en_US_POSIX")
    private static let maxCoefficient: [UInt8] = Array("79228162514264337593543950335".utf8)

    static func isValid(_ bytes: UnsafeBufferPointer<UInt8>) -> Bool {
        let count = bytes.count
        guard count >= 1, count <= 64 else { return false }
        @inline(__always) func isDigit(_ byte: UInt8) -> Bool { byte >= 0x30 && byte <= 0x39 }
        var index = 0
        if bytes[0] == 0x2D { index = 1 }
        guard index < count else { return false }
        let digitsStart = index
        if bytes[index] == 0x30 {
            index += 1
        } else {
            guard bytes[index] >= 0x31, bytes[index] <= 0x39 else { return false }
            while index < count, isDigit(bytes[index]) { index += 1 }
        }
        var dotIndex = -1
        if index < count {
            guard bytes[index] == 0x2E else { return false }
            dotIndex = index
            let fractionLength = count - index - 1
            guard fractionLength >= 1, fractionLength <= 28 else { return false }
            var cursor = index + 1
            while cursor < count {
                guard isDigit(bytes[cursor]) else { return false }
                cursor += 1
            }
        }
        var significantCount = 0
        var comparison = 0
        for cursor in digitsStart..<count where cursor != dotIndex {
            let digit = bytes[cursor]
            if significantCount == 0 && digit == 0x30 { continue }
            if significantCount >= 29 { return false }
            if comparison == 0 {
                let bound = maxCoefficient[significantCount]
                if digit < bound { comparison = -1 } else if digit > bound { comparison = 1 }
            }
            significantCount += 1
        }
        return !(significantCount == 29 && comparison == 1)
    }

    static func isValid(_ text: String) -> Bool {
        var copy = text
        return copy.withUTF8 { isValid($0) }
    }

    static func parse(_ text: String) -> Decimal? {
        guard isValid(text),
              let parsed = Decimal(string: text, locale: posixLocale),
              !parsed.isNaN else { return nil }
        return parsed
    }
}
