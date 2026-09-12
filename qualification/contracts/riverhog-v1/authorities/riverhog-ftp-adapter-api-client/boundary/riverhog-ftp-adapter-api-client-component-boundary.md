# riverhog-ftp-adapter-api-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-api-client-component-boundary:080243d900 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter-api-client` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/28`

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
    "http-api-contracts"
  ],
  "distribution": "riverhog-ftp-adapter-api-client",
  "optional_dependencies": {},
  "path": "reference/riverhog/ingress/ftp-api-client",
  "role": "reference_component"
}
```
