# Operation parity: create_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-create-retrieval-job:5c3dd69b44 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `retrieval-jobs` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/96`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [POST /v1/retrieval-jobs](../http/post-v1-retrieval-jobs.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | ["local repair", "local sync"] |
| `client` | ApiClient |
| `method` | POST |
| `operation_id` | create_retrieval_job |
| `path` | /v1/retrieval-jobs |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 03bf3338113e196bf724178532a6dd3c8565847fd5a023b49230d4d792ca5782 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "create_retrieval_job",
  "path": "/v1/retrieval-jobs",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
