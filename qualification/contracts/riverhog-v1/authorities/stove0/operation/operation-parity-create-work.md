# Operation parity: create_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-create-work:736597124e -->

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
| `cli_commands` | ["work create"] |
| `client` | Stove0ApiClient |
| `method` | POST |
| `operation_id` | create_work |
| `path` | /v1/work |
| `provider_evidence` | None |
| `read_collection` | None |
| `response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/work](../http/post-v1-work.md)
- [stove0 work create](../cli/stove0-work-create.md)

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

- `/external_contract/operations/140`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7becd13088668454120980b3a36b5e94e0abfd52f4911c50a123bc595b8a1518 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "work create"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "create_work",
  "path": "/v1/work",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
