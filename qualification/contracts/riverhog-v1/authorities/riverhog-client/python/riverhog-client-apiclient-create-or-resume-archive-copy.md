# riverhog_client.ApiClient.create_or_resume_archive_copy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-create-or-resum-01171bf6e1:da17c26d04 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3f1c426088"></a>
- <a id="s-c13c6abff5"></a>`distribution`: `riverhog-client`
- <a id="s-ada2e589a3"></a>`module`: `riverhog_client`
- <a id="s-6d4533a4c4"></a>`name`: `create_or_resume_archive_copy`
- <a id="s-b4d4987a20"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-f1ec8f158e"></a>`unit`: `member`

### Declared structure

- <a id="s-515b0eab9e"></a>`kind`: `"method"`
- <a id="s-36ff232635"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, destination_store: 'ArchiveStoreName', source_store: 'ArchiveStoreName \| None' = None, event_context: 'Mapping[str, Any] \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity archive copy start](../../piggity/cli/piggity-archive-copy-start.md)
- [POST /v1/archive/copies](../../riverhog/http-operations/post-v1-archive-copies.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-7acdf795d4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.create\_or\_resume\_archive\_copy](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2409)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.create_or_resume_archive_copy`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90cedef088550cf5ce03770838692c5104dabe18e573f779a2ca780a6e4c030c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, destination_store: 'ArchiveStoreName', source_store: 'ArchiveStoreName | None' = None, event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_or_resume_archive_copy",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
