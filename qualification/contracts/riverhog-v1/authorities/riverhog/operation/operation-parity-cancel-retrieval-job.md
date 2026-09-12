# Operation parity: cancel_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-cancel-retrieval-job:a0a433d9c6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [retrieval-jobs](families/retrieval-jobs/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3c085132aa"></a>
| Concern | Contract |
|---|---|
| <a id="s-655f78f028"></a>`application` | riverhog |
| <a id="s-f815a7c79d"></a>`classification` | client-only-primitive |
| <a id="s-79f1421fa6"></a>`cli_commands` | ["local evict", "local remove"] |
| <a id="s-88099697b5"></a>`client` | ApiClient |
| <a id="s-33f7c2a8db"></a>`method` | DELETE |
| <a id="s-1c187c445b"></a>`operation_id` | cancel_retrieval_job |
| <a id="s-5e97f419a6"></a>`path` | /v1/retrieval-jobs/{job_id} |
| <a id="s-d24f775e3e"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-e7b1361b9e"></a>`read_collection` | None |
| <a id="s-80342824a2"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [DELETE /v1/retrieval-jobs/{job_id}](../http/delete-v1-retrieval-jobs-job-id.md)
- [piggity local evict](../../piggity/cli/piggity-local-evict.md)
- [piggity local remove](../../piggity/cli/piggity-local-remove.md)

## Governing policies

- <a id="pa-cd97e3fbeb"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-bfb0742291"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-bc3c7ada0c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/97`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a80f6050751748ab5d39d17996d10b7f96f11b6aa047adb239875f1f95d7445 -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "local evict",
    "local remove"
  ],
  "client": "ApiClient",
  "method": "DELETE",
  "operation_id": "cancel_retrieval_job",
  "path": "/v1/retrieval-jobs/{job_id}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
