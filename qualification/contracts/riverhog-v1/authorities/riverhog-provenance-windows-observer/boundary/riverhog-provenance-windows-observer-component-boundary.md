# riverhog-provenance-windows-observer component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-windows-observer:riverhog-provenance-windows-observer-comp-f14e00eb8c:19842e4df0 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-observer` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/34`

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
    "riverhog-provenance-windows-contracts"
  ],
  "distribution": "riverhog-provenance-windows-observer",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/observers/windows",
  "role": "reference_component"
}
```
