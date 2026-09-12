# Operation parity: plan_collection_upload_discard

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-plan-collection-upload-discard:7ef5571968 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-faaf29051f5b"></a>
| Concern | Contract |
|---|---|
| <a id="s-64391010abd4"></a>`application` | riverhog |
| <a id="s-6614a5cb00fb"></a>`classification` | human-cli+json |
| <a id="s-628360854501"></a>`cli_commands` | ["collection upload discard"] |
| <a id="s-f7275537e741"></a>`client` | ApiClient |
| <a id="s-2cc6e1f98285"></a>`method` | POST |
| <a id="s-6cc7516e9598"></a>`operation_id` | plan_collection_upload_discard |
| <a id="s-f5f8cc999502"></a>`path` | /v1/collection-upload-sessions/{collection_id}/discard-plan |
| <a id="s-500b77d390c3"></a>`provider_evidence` | None |
| <a id="s-6851ab665eaa"></a>`read_collection` | None |
| <a id="s-a40d0441a453"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collection-upload-sessions/{collection_id}/discard-plan](../http/post-v1-collection-upload-sessions-collection-id-discard-plan.md)
- [piggity collection upload discard](../../piggity/cli/piggity-collection-upload-discard.md)

## Governing policies

- <a id="pa-ed92bff2398e"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-80037a927e0e"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-ebc01811f9cf"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/57`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba60b3d8ef9ed2552e273a51f54825dd017c1f965b0f35c6d7cb8118dc7d6f62 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection upload discard"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "plan_collection_upload_discard",
  "path": "/v1/collection-upload-sessions/{collection_id}/discard-plan",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "http-json"
}
```
