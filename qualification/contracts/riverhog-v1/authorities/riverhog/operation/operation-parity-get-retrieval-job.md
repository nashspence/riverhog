# Operation parity: get_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-retrieval-job:635e74a52e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [retrieval-jobs](families/retrieval-jobs/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c9427118fd12"></a>
| Concern | Contract |
|---|---|
| <a id="s-fe0a95d581b9"></a>`application` | riverhog |
| <a id="s-df8fde2aa952"></a>`classification` | client-only-primitive |
| <a id="s-af987b3b6369"></a>`cli_commands` | ["local evict", "local remove", "local repair", "local sync"] |
| <a id="s-53b6b67620e1"></a>`client` | ApiClient |
| <a id="s-edbddce2591d"></a>`method` | GET |
| <a id="s-2a163b85c7c0"></a>`operation_id` | get_retrieval_job |
| <a id="s-71388745880e"></a>`path` | /v1/retrieval-jobs/{job_id} |
| <a id="s-68fdd2524bdb"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-d16e2fe0a72b"></a>`read_collection` | None |
| <a id="s-d7658acd6769"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-jobs/{job_id}](../http/get-v1-retrieval-jobs-job-id.md)
- [piggity local evict](../../piggity/cli/piggity-local-evict.md)
- [piggity local remove](../../piggity/cli/piggity-local-remove.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Governing policies

- <a id="pa-50503842235f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-0e86cf70b6b0"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-914b666e1506"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/98`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eb6388fd19b67aa937620b0ba6ea97b609f2ea27846e78d2f8f47c000e17d667 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "local evict",
    "local remove",
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_retrieval_job",
  "path": "/v1/retrieval-jobs/{job_id}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
