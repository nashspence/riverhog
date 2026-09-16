# riverhog_client.ApiClient.register_collection_upload_session_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-register-collec-9e5556ef67:f5ae886535 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0de7b8093b"></a>
- <a id="s-6b68f7491f"></a>`distribution`: `riverhog-client`
- <a id="s-5da7530d7a"></a>`module`: `riverhog_client`
- <a id="s-eca759b822"></a>`name`: `register_collection_upload_session_files`
- <a id="s-a56b487db9"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-86096fcba2"></a>`unit`: `member`

### Declared structure

- <a id="s-19daacba5b"></a>`kind`: `"method"`
- <a id="s-31b0c3d2da"></a>`signature`: `"\"(self, collection_id: 'CollectionId', files: 'Sequence[CollectionUploadFileIn \| Mapping[str, Any]]', *, registration_constraints: 'CollectionUploadRegistrationConstraintsDocument \| Mapping[str, Any]') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity collection upload start](../../piggity/cli/piggity-collection-upload-start.md)
- [POST /v1/collection-upload-sessions/{collection_id}/files](../../riverhog/http-operations/post-v1-collection-upload-sessions-collection-id-files.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-8f12ecfd9a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/client.py::ApiClient.register_collection_upload_session_files](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1166)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.register_collection_upload_session_files`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e04437e46015374884b1df468dc7eb24446dccfca006548164a50beffd9d5ec5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', files: 'Sequence[CollectionUploadFileIn | Mapping[str, Any]]', *, registration_constraints: 'CollectionUploadRegistrationConstraintsDocument | Mapping[str, Any]') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "register_collection_upload_session_files",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
