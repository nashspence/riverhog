# Operation parity: retry_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-retry-work:aaa9396625 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `operation` |
| Family | `work` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | stove0 |
| `classification` | human-cli+json |
| `cli_commands` | ["work retry"] |
| `client` | Stove0ApiClient |
| `method` | POST |
| `operation_id` | retry_work |
| `path` | /v1/work/{work_id}/retry |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/work/{work_id}/retry](../http/post-v1-work-work-id-retry.md)
- [stove0 work retry](../cli/stove0-work-retry.md)

## Governing policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/144`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c9475cadb8994944c85f857f04c2120b3eb6117ac13e6c2172e62a680825ff5 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "work retry"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "retry_work",
  "path": "/v1/work/{work_id}/retry",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
