# Operation parity: renew_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-renew-retrieval-job:64f30c747d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [retrieval-jobs](families/retrieval-jobs/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1b994d29e1"></a>
| Concern | Contract |
|---|---|
| <a id="s-9f6089925e"></a>`application` | riverhog |
| <a id="s-cac40f72e6"></a>`classification` | client-only-primitive |
| <a id="s-5167893b4c"></a>`cli_commands` | ["local repair", "local sync"] |
| <a id="s-79aa86d760"></a>`client` | ApiClient |
| <a id="s-55975abc25"></a>`method` | POST |
| <a id="s-fd72338373"></a>`operation_id` | renew_retrieval_job |
| <a id="s-8cbfc4e359"></a>`path` | /v1/retrieval-jobs/{job_id}/renew |
| <a id="s-9e93f1de22"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-329a6d8d92"></a>`read_collection` | None |
| <a id="s-5add7c69ab"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/retrieval-jobs/{job_id}/renew](../http/post-v1-retrieval-jobs-job-id-renew.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Governing policies

- <a id="pa-c9bb9442b7"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c03ddc410f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-ddd6b67c0b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/102`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a56e7a6bf941cee0b5b603b4a658cfecb7fe4f55bb1e89fe295f135f40cfe8ff -->

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
  "operation_id": "renew_retrieval_job",
  "path": "/v1/retrieval-jobs/{job_id}/renew",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
