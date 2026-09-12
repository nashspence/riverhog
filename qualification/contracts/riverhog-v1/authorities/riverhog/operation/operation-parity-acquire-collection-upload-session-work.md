# Operation parity: acquire_collection_upload_session_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-acquire-collection-uploa-37b8920762:66e4942fd7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d1a413f3eab4"></a>
| Concern | Contract |
|---|---|
| <a id="s-8669091f0685"></a>`application` | riverhog |
| <a id="s-015a3aac1f1b"></a>`classification` | client-only-primitive |
| <a id="s-37705efe9dc5"></a>`cli_commands` | ["collection upload start"] |
| <a id="s-57355cf53854"></a>`client` | ApiClient |
| <a id="s-0e84aea4d7f9"></a>`method` | GET |
| <a id="s-373821c45938"></a>`operation_id` | acquire_collection_upload_session_work |
| <a id="s-ee5c37497efd"></a>`path` | /v1/collection-upload-sessions/{collection_id}/work |
| <a id="s-75d4074ff3e2"></a>`provider_evidence` | None |
| <a id="s-878c8b3f4cd8"></a>`read_collection` | None |
| <a id="s-803613cb6f4a"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}/work](../http/get-v1-collection-upload-sessions-collection-id-work.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Governing policies

- <a id="pa-2d9ad96b2ee3"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-f1735a41d099"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-6343b9ce3d0b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/69`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1f7e5d99e65daeeeb760bc150b95325f1d1efcf14483997108c4bdbf1bfee19d -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "collection upload start"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "acquire_collection_upload_session_work",
  "path": "/v1/collection-upload-sessions/{collection_id}/work",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
