# riverhog_client.ApiClient.list_collection_archive_copies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-collection-db295c0400:5e3cf3974f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0d61b006bf"></a>
- <a id="s-108427f19a"></a>`distribution`: `riverhog-client`
- <a id="s-f05cbfa645"></a>`module`: `riverhog_client`
- <a id="s-09dd9ec1a8"></a>`name`: `list_collection_archive_copies`
- <a id="s-cfc6f8c5d6"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-d1c99e6602"></a>`unit`: `member`

### Declared structure

- <a id="s-6093d895db"></a>`kind`: `"method"`
- <a id="s-2a63e92612"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, page_size: 'int' = 25, page_token: 'str \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity collection archive-copies](../../piggity/cli/piggity-collection-archive-copies.md)
- [GET /v1/collections/{collection_id}/archive-copies](../../riverhog/http-operations/get-v1-collections-collection-id-archive-copies.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-18cca84f5e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.list\_collection\_archive\_copies](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1604)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_collection_archive_copies`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 49c3b7ab8d370078be75828cb7bc5bf80ef95a708b6ae6e54f43b903fa5bb053 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_collection_archive_copies",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
