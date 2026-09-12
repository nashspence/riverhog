# gogurt-listener durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:gogurt-listener:gogurt-listener-durable-state:11bfac7224 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `gogurt-listener` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `format`: gogurt-listener-state/v1

## Governing policies

- `compatibility/components/v1`

## Evidence

### Qualification

- `make release-check`
- `make database-qualification`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:gogurt-listener` — `state:gogurt-listener`

### Machine authority

- `/external_contract/durable_state/owners/4`

### Exact owned JSON

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
