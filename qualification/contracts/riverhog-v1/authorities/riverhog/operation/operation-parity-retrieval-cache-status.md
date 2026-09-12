# Operation parity: retrieval_cache_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-retrieval-cache-status:e4ad867bbd -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `retrieval-cache` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/operations/93`

## Effective policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`
- Proof: `make operation-qualification`

## Related interface records

- [GET /v1/retrieval-cache](../http/get-v1-retrieval-cache.md)
- [piggity retrieval cache status](../../piggity/cli/piggity-retrieval-cache-status.md)

## Contract summary

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | human-cli+json |
| `cli_commands` | ["retrieval cache status"] |
| `client` | ApiClient |
| `method` | GET |
| `operation_id` | retrieval_cache_status |
| `path` | /v1/retrieval-cache |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | http-json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7fa39ad8f54924de3d6f307196a23dd7d8bb34c661dd64763376b26def7b16e5 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "retrieval cache status"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "retrieval_cache_status",
  "path": "/v1/retrieval-cache",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
