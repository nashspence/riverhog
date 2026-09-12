# Operation parity: put_collection_upload_session_unit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:riverhog:operation-parity-put-collection-upload-session-unit:6b4dc13375 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `operation` |
| Family | `collection-upload-sessions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Concern | Contract |
|---|---|
| `application` | riverhog |
| `classification` | client-only-primitive |
| `cli_commands` | ["collection upload start"] |
| `client` | ApiClient |
| `method` | PUT |
| `operation_id` | put_collection_upload_session_unit |
| `path` | /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit} |
| `provider_evidence` | provider-qualification:#442 |
| `read_collection` | None |
| `response_authority` | canonical-document |

## Maintained corroboration

### Related interface records

- [PUT /v1/collection-upload-sessions/{collection_id}/volumes/{volume_id}/units/{unit}](../http/put-v1-collection-upload-sessions-collection-id-volumes-volume-id-units-unit.md)
- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)

## Governing policies

- `compatibility/cli/v1`
- `compatibility/components/v1`
- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `operations:operation-matrix` — `scripts/operation_qualification.py::operation_matrix`

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
