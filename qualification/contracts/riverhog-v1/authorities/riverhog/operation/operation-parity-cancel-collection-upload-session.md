# Operation parity: cancel_collection_upload_session

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-cancel-collection-upload-session:b4906f637f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-333c776250"></a>
| Concern | Contract |
|---|---|
| <a id="s-56c39453ad"></a>`application` | riverhog |
| <a id="s-3dcdb435d5"></a>`classification` | human-cli+json |
| <a id="s-4637fa8a83"></a>`cli_commands` | ["collection upload cancel"] |
| <a id="s-21c362d37c"></a>`client` | ApiClient |
| <a id="s-572d0ef8f5"></a>`method` | POST |
| <a id="s-9edd5b99f0"></a>`operation_id` | cancel_collection_upload_session |
| <a id="s-c070b41950"></a>`path` | /v1/collection-upload-sessions/{collection_id}/cancel |
| <a id="s-b4260225a1"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-25b096b3d8"></a>`read_collection` | None |
| <a id="s-1af84d7b20"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/cancel](../http/post-v1-collection-upload-sessions-collection-id-cancel.md)
- [piggity collection upload cancel](../../piggity/cli/piggity-collection-upload-cancel.md)

## Governing policies

- <a id="pa-c4a051211b"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-1b3da3fcb5"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-b92b1882b4"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/54`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e2cf867f13b119a0942aa86c804878a7972ea61c79591ad74e8bccdb060787a -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection upload cancel"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "cancel_collection_upload_session",
  "path": "/v1/collection-upload-sessions/{collection_id}/cancel",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
