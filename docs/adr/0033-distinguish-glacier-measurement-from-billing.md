# ADR-0033: Distinguish Glacier Measurement from Billing

## Decision

Riverhog reports measured archive storage separately from estimated billing, AWS actuals, forecasts, exports, and invoices.

## Reason

Operational archive state and cloud billing evidence answer different questions and should not be conflated.
