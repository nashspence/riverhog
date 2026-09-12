# mango-fish-cursor durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:mango-fish-cursor:mango-fish-cursor-durable-state:2504b0617b -->

| Audit field | Value |
|---|---|
| Authority | `mango-fish-cursor` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/durable_state/owners/2`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:mango-fish-cursor` — `state:mango-fish-cursor`
- Proof: `make release-check`
- Proof: `make database-qualification`

## Contract summary

- `format`: state-schema/sqlite

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 957ea6c69fd3f899e5f9f0053f31445028a07f80bc240b34fbf70797df14b586 -->

```json
{
  "distribution": "mango-fish",
  "fixture_sha256s": [
    "f1baf4752b190b555de143a53a7b68eb41785895fa1d353c86e90b111fa6bf96"
  ],
  "format": "state-schema/sqlite",
  "head": "v1_0001",
  "id": "mango-fish-cursor"
}
```
