# riverhog_client.ApiClient.collection_contains_tag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-collection-contains-tag:9fb6ec20d2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-df54285ca2"></a>
- <a id="s-2a2439861c"></a>`distribution`: `riverhog-client`
- <a id="s-bad671c861"></a>`module`: `riverhog_client`
- <a id="s-c0d3fcb7d0"></a>`name`: `collection_contains_tag`
- <a id="s-d655b05750"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-51f40f18a3"></a>`unit`: `member`

### Declared structure

- <a id="s-57cdd19e46"></a>`kind`: `"method"`
- <a id="s-286e9f3794"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, tag: 'CollectionTag', revision: 'int', tag_set_identity: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity collection tag contains](../../piggity/cli/piggity-collection-tag-contains.md)
- [POST /v1/collections/{collection_id}/tags:contains](../../riverhog/http-operations/post-v1-collections-collection-id-tags-contains.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-d98a923691"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.collection\_contains\_tag](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2269)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.collection_contains_tag`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1f4c1b89eb002dec7edbe0366ae21bca5c9cf8e6545fddbfeeb660433a35dc9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, tag: 'CollectionTag', revision: 'int', tag_set_identity: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "collection_contains_tag",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
