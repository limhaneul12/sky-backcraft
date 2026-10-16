import Foundation

/// Owns the ISO-8601 formatters for one synchronous read/decode invocation.
/// Callers must create a fresh parser for each invocation and pass it through row decoders.
final class ReadTimestampParser {
    private let ordinary: ISO8601DateFormatter
    private let fractional: ISO8601DateFormatter

    init() {
        ordinary = ISO8601DateFormatter()
        let fractional = ISO8601DateFormatter()
        fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        self.fractional = fractional
    }

    func ordinaryThenFractional(_ value: String) -> Date? {
        ordinary.date(from: value) ?? fractional.date(from: value)
    }

    func fractionalThenOrdinary(_ value: String) -> Date? {
        fractional.date(from: value) ?? ordinary.date(from: value)
    }
}
