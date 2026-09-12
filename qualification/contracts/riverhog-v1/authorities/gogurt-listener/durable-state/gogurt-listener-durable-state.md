# gogurt-listener durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:gogurt-listener:gogurt-listener-durable-state:11bfac7224 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-listener` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/durable_state/owners/4`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:gogurt-listener` — `state:gogurt-listener`
- Proof: `make release-check`
- Proof: `make database-qualification`

## Contract summary

- `format`: gogurt-listener-state/v1

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8a324964b8ea8088c138048b7c54a67ab0944954314a1f147f44ab64dd1b38f4 -->

```json
{
  "distribution": "gogurt-listener-runtime",
  "fixture_sha256s": [
    "278ff26b6357b89c6af934f31b31917557cd3c377ecdddf8308eb3d9962cd15a"
  ],
  "format": "gogurt-listener-state/v1",
  "head": "1",
  "id": "gogurt-listener"
}
```
