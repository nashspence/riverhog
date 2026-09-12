# riverhog-provenance-linux-observer component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-linux-observer:riverhog-provenance-linux-observer-compon-61290e85ef:09854638c0 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-linux-observer` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/32`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-provenance",
    "riverhog-provenance-linux-contracts"
  ],
  "distribution": "riverhog-provenance-linux-observer",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/observers/linux",
  "role": "reference_component"
}
```
