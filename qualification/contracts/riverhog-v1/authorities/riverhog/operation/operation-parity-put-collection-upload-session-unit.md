# Operation parity: put_collection_upload_session_unit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-put-collection-upload-session-unit:6b4dc13375 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-67c22427c4"></a>
| Concern | Contract |
|---|---|
| <a id="s-a69c2c2be8"></a>`application` | riverhog |
| <a id="s-61eebc9947"></a>`classification` | client-only-primitive |
| <a id="s-385044d9e8"></a>`cli_commands` | ["collection upload start"] |
| <a id="s-647dd02838"></a>`client` | ApiClient |
| <a id="s-edd7dff9fe"></a>`method` | PUT |
| <a id="s-3fbdf6aa5e"></a>`operation_id` | put_collection_upload_session_unit |
| <a id="s-5b256fbf6d"></a>`path` | /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit} |
| <a id="s-256b158fb8"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-ecb183cfec"></a>`read_collection` | None |
| <a id="s-1720fd5264"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [PUT /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}](../http/put-v1-collection-upload-sessions-collection-id-volumes-volume-id-units-unit.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Governing policies

- <a id="pa-b9551706d2"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-44fe736b03"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-bc7e9d3d9a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/68`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da06e5f32e5dd8d587173bf5a352365333387c9995a0bef19c33fae42e21b64c -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "collection upload start"
  ],
  "client": "ApiClient",
  "method": "PUT",
  "operation_id": "put_collection_upload_session_unit",
  "path": "/v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
