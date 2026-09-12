# Operation parity: cancel_archive_copy_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-cancel-archive-copy-job:10b40b9177 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [archive](families/archive/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f4287e7366"></a>
| Concern | Contract |
|---|---|
| <a id="s-e1c8acb6f6"></a>`application` | riverhog |
| <a id="s-003e12ed72"></a>`classification` | human-cli+json |
| <a id="s-c912455a82"></a>`cli_commands` | ["archive copy cancel"] |
| <a id="s-01982eb65d"></a>`client` | ApiClient |
| <a id="s-f55048cb3c"></a>`method` | DELETE |
| <a id="s-a82b1424d7"></a>`operation_id` | cancel_archive_copy_job |
| <a id="s-d4ac80140e"></a>`path` | /v1/archive/copies/{collection_id}/{destination_store} |
| <a id="s-634b94fe94"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-fb807699c5"></a>`read_collection` | None |
| <a id="s-22b895072a"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [DELETE /v1/archive/copies/{collection_id}/{destination_store}](../http/delete-v1-archive-copies-collection-id-destination-store.md)
- [piggity archive copy cancel](../../piggity/cli/piggity-archive-copy-cancel.md)

## Governing policies

- <a id="pa-acd86d7b6b"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-59d9713d83"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-98ce9565e0"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/16`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da66bcb27abaea166a31f55049f3ecee442e520e16352e7e5b772f5f688319c5 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "archive copy cancel"
  ],
  "client": "ApiClient",
  "method": "DELETE",
  "operation_id": "cancel_archive_copy_job",
  "path": "/v1/archive/copies/{collection_id}/{destination_store}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
