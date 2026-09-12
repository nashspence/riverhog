# Operation parity: download_retrieval_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-download-retrieval-file:7a482c2b2b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [retrieval-jobs](families/retrieval-jobs/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-99292f23e734"></a>
| Concern | Contract |
|---|---|
| <a id="s-59a79f2bfb2a"></a>`application` | riverhog |
| <a id="s-b0f1222835f7"></a>`classification` | client-only-primitive |
| <a id="s-f8e440b8b08b"></a>`cli_commands` | ["local repair", "local sync"] |
| <a id="s-e25ba8b4838a"></a>`client` | ApiClient |
| <a id="s-73d31b3f735c"></a>`method` | GET |
| <a id="s-1cc5119af982"></a>`operation_id` | download_retrieval_file |
| <a id="s-32a998787638"></a>`path` | /v1/retrieval-jobs/{job_id}/content |
| <a id="s-b07869c4775d"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-b70a2cc1a8ee"></a>`read_collection` | None |
| <a id="s-28b415951a37"></a>`response_authority` | stream-or-empty |

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-jobs/{job_id}/content](../http/get-v1-retrieval-jobs-job-id-content.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Governing policies

- <a id="pa-50b03c28e85f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-83fdce62c87e"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-e42594e13b4e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/100`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e58d21f1ee66619f35c79ccfa757b30b5998a809ded57441bad96a46906e840e -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "local repair",
    "local sync"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "download_retrieval_file",
  "path": "/v1/retrieval-jobs/{job_id}/content",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "stream-or-empty"
}
```
