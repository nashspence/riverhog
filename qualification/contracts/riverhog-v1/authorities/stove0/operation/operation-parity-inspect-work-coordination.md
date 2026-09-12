# Operation parity: inspect_work_coordination

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-inspect-work-coordination:76362d78bd -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `work` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/143`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/work/{work_id}/coordination](../http/get-v1-work-work-id-coordination.md)
- [stove0 work coordination](../cli/stove0-work-coordination.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["work coordination"] |
| `client` | Stove0ApiClient |
| `method` | GET |
| `operation_id` | inspect_work_coordination |
| `path` | /v1/work/{work_id}/coordination |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | canonical-document |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d7a1df3778cbea5bb1aa22e8bfba8b0c4bec6cc63150813dc36b7e5d4e93c336 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "work coordination"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "inspect_work_coordination",
  "path": "/v1/work/{work_id}/coordination",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
