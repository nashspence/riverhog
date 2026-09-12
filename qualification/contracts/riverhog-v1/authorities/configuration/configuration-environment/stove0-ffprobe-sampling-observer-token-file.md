# STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffprobe-sampling-observer-token-file:375fce7bbf -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/102`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE` — `configuration-environment:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract summary

| Field | Shape |
|---|---|
| `consumers` | array (1 items) |
| `name` | "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 68ff9f5ee242a4704edf78a7ce1c3afa36b416e9bdd880bb4fc4abd20d62374e -->

```json
{
  "consumers": [
    "stove0-ffprobe-sampling-observer"
  ],
  "name": "STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE"
}
```
