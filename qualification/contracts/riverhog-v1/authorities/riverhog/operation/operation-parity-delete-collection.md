# Operation parity: delete_collection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-delete-collection:55562362c0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collections](families/collections/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-815b3471fcc7"></a>
| Concern | Contract |
|---|---|
| <a id="s-979f6bf81454"></a>`application` | riverhog |
| <a id="s-52cebca733e9"></a>`classification` | human-cli+json |
| <a id="s-fca2d27fcd58"></a>`cli_commands` | ["collection delete"] |
| <a id="s-cae19c51f9a9"></a>`client` | ApiClient |
| <a id="s-fabc4dbc6a19"></a>`method` | POST |
| <a id="s-ca3b73102565"></a>`operation_id` | delete_collection |
| <a id="s-585491ab3a21"></a>`path` | /v1/collections/{collection_id}/delete |
| <a id="s-f03f3abb4a1d"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-2301d4446dc0"></a>`read_collection` | None |
| <a id="s-fcd1c4858446"></a>`response_authority` | http-json |

## Maintained corroboration

### Related interface records

- [POST /v1/collections/{collection_id}/delete](../http/post-v1-collections-collection-id-delete.md)
- [piggity collection delete](../../piggity/cli/piggity-collection-delete.md)

## Governing policies

- <a id="pa-96b821c0c7f5"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-1f717345c177"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-774964f8f835"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/73`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d777f37738101b07d05bf08df890f121abe16efb7f12757119392befac4945d9 -->

```json
{
  "application": "riverhog",
  "classification": "human-cli+json",
  "cli_commands": [
    "collection delete"
  ],
  "client": "ApiClient",
  "method": "POST",
  "operation_id": "delete_collection",
  "path": "/v1/collections/{collection_id}/delete",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "http-json"
}
```
