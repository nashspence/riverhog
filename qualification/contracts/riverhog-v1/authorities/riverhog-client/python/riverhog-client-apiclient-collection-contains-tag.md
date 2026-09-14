# riverhog_client.ApiClient.collection_contains_tag

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-collection-contains-tag:9fb6ec20d2 -->

Exact externally visible contract owned by this semantic dossier.

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

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-d98a923691"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.collection_contains_tag`

### Exact owned JSON

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
