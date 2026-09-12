# Operation parity: get_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-get-work:eba7ff859e -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `work` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/141`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/work/{work_id}](../http/get-v1-work-work-id.md)
- [stove0 work show](../cli/stove0-work-show.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["work show"] |
| `client` | Stove0ApiClient |
| `method` | GET |
| `operation_id` | get_work |
| `path` | /v1/work/{work_id} |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a061abb3a64732deb3c022132a74f84feba5091b6d5d094ba64e30be352cc140 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "work show"
  ],
  "client": "Stove0ApiClient",
  "method": "GET",
  "operation_id": "get_work",
  "path": "/v1/work/{work_id}",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
