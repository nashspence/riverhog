# stove0-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-observer-protocol:stove0-observer:4b992ab432 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-protocol` |
| Interface | `boundary` |
| Family | `process-extensions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/process_extensions/1`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract

- `protocols`: ["stove0-content-observer/v1"]
