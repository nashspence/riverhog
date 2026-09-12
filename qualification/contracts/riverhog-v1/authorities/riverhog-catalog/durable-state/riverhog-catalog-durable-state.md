# riverhog-catalog durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-durable-state:e07af31f57 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-catalog` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/durable_state/owners/0`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:riverhog-catalog` — `state:riverhog-catalog`
- Proof: `make release-check`
- Proof: `make database-qualification`

## Contract summary

- `format`: state-schema/postgresql

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c6f90a3b0c440c77926b608ea9007e4d5153484f7a506f5eaac7660fa8166e08 -->

```json
{
  "distribution": "riverhog-server",
  "fixture_sha256s": [
    "8b337f69f6bdc2665afd1b24a645301878925997d1a4416b607c82204d68bc96"
  ],
  "format": "state-schema/postgresql",
  "head": "v1_0001",
  "id": "riverhog-catalog"
}
```
