# riverhog-ftp-adapter component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-ftp-adapter:riverhog-ftp-adapter-component-boundary:33f5955bfe -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/27`

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
  "console_scripts": {
    "riverhog-ftp-adapter": "riverhog_ftp_adapter.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "riverhog-ftp-adapter-api-client",
    "riverhog-protocol",
    "riverhog-provenance"
  ],
  "distribution": "riverhog-ftp-adapter",
  "optional_dependencies": {},
  "path": "reference/riverhog/ingress/ftp",
  "role": "reference_component"
}
```
