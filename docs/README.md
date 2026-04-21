# Documentation

This repository uses a split between proposal documents, durable product/architecture docs, reference docs,
architecture decision records, and historical migration notes.

## Layout

- `rfcs/` — proposals that explain the problem, constraints, and the chosen direction before the design is fully settled
- `explanation/` — durable conceptual documentation explaining why the system exists and how it works
- `reference/` — normative behavior, contracts, grammars, and state models
- `adr/` — one significant architectural or behavioral decision per file
- `how-to/` — task-oriented usage guides

## Source of truth

- The current product and architecture story lives in `explanation/`
- The current external contract lives in `reference/` and `openapi/arc.v1.yaml`
- The project decision log lives in `adr/`
- Historical donor/transplant notes live in `archive/`

## Conventions

- Use numbered IDs only for RFCs and ADRs.
- Keep stable file names based on topic, not on drafting sequence.
- Put normative API behavior in reference docs and machine-readable specs.
- Keep executable acceptance criteria under `tests/acceptance/`.
