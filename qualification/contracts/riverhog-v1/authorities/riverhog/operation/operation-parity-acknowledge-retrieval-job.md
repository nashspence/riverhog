# Operation parity: acknowledge_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-acknowledge-retrieval-job:5c67a196c8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [retrieval-jobs](families/retrieval-jobs/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-44071ca5f7da"></a>
| Concern | Contract |
|---|---|
| <a id="s-47901ca51f5e"></a>`application` | riverhog |
| <a id="s-957b2ed0b740"></a>`classification` | client-only-primitive |
| <a id="s-6f16664b6678"></a>`cli_commands` | ["local repair", "local sync"] |
| <a id="s-13b5c20dcca9"></a>`client` | ApiClient |
| <a id="s-952ad768e55b"></a>`method` | POST |
| <a id="s-243ae858a4f6"></a>`operation_id` | acknowledge_retrieval_job |
| <a id="s-e504ed8396e2"></a>`path` | /v1/retrieval-jobs/{job_id}/ack |
| <a id="s-83ac6cc4f0f6"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-69789fcb724f"></a>`read_collection` | None |
| <a id="s-eba02e297f36"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/retrieval-jobs/{job_id}/ack](../http/post-v1-retrieval-jobs-job-id-ack.md)
- [piggity local repair](../../piggity/cli/piggity-local-repair.md)
- [piggity local sync](../../piggity/cli/piggity-local-sync.md)

## Governing policies

- <a id="pa-15964aa924c9"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-f49f53d15e93"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-f5f6f13732be"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/99`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a394ba1f88c8b82b3511d8d060a61c86b458b1baced6169109d54850880a600f -->

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
  "operation_id": "acknowledge_retrieval_job",
  "path": "/v1/retrieval-jobs/{job_id}/ack",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
