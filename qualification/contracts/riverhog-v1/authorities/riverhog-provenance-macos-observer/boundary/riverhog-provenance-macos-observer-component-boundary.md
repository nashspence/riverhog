# riverhog-provenance-macos-observer component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-macos-observer:riverhog-provenance-macos-observer-compon-f6fe932ece:958fc43472 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-macos-observer` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/33`

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
    "riverhog-provenance-macos-contracts"
  ],
  "distribution": "riverhog-provenance-macos-observer",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/observers/macos",
  "role": "reference_component"
}
```
