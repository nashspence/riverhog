# Operation parity: get_collection_upload_session_unit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-get-collection-upload-session-unit:39de2e49f9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [operation](index.md) |
| Family | [collection-upload-sessions](families/collection-upload-sessions/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c91c60b9ca96"></a>
| Concern | Contract |
|---|---|
| <a id="s-46a6a3a6431f"></a>`application` | riverhog |
| <a id="s-c9be3adc3103"></a>`classification` | client-only-primitive |
| <a id="s-63950cd55473"></a>`cli_commands` | ["collection upload start"] |
| <a id="s-f85f08b0a57f"></a>`client` | ApiClient |
| <a id="s-32a4ea708fb8"></a>`method` | GET |
| <a id="s-62526cdef008"></a>`operation_id` | get_collection_upload_session_unit |
| <a id="s-f29058d9c510"></a>`path` | /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit} |
| <a id="s-c870659cb1b1"></a>`provider_evidence` | provider-qualification:#442 |
| <a id="s-b4139a2fd069"></a>`read_collection` | None |
| <a id="s-75db8850fbed"></a>`response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}](../http/get-v1-collection-upload-sessions-collection-id-volumes-volume-id-units-unit.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Governing policies

- <a id="pa-d2ae2955d7bf"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-6252bcedc1c4"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-96c67e3860e4"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/67`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 164af302a516770a101bb209dabe74684f78d2dc6991b7a7e9fc7d68e4b5a94e -->

```json
{
  "application": "riverhog",
  "classification": "client-only-primitive",
  "cli_commands": [
    "collection upload start"
  ],
  "client": "ApiClient",
  "method": "GET",
  "operation_id": "get_collection_upload_session_unit",
  "path": "/v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}",
  "provider_evidence": "provider-qualification:#442",
  "read_collection": null,
  "response_authority": "canonical-document"
}
```
