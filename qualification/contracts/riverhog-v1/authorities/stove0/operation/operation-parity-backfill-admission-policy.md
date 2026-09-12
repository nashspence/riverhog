# Operation parity: backfill_admission_policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-backfill-admission-policy:efa04c17d1 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `admission-policies` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/119`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/admission-policies/{policy_id}:backfill](../http/post-v1-admission-policies-policy-id-backfill.md)
- [stove0 admission policy backfill](../cli/stove0-admission-policy-backfill.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["admission policy backfill"] |
| `client` | Stove0ApiClient |
| `method` | POST |
| `operation_id` | backfill_admission_policy |
| `path` | /v1/admission-policies/{policy_id}:backfill |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4ec8d7c36da38a53f7db66c1a1c91b876aaad847f9b43337f0d94e44f87556e6 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "admission policy backfill"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "backfill_admission_policy",
  "path": "/v1/admission-policies/{policy_id}:backfill",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
