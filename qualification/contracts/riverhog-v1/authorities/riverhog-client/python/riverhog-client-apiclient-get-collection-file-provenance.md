# riverhog_client.ApiClient.get_collection_file_provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-collection-d7fc7f5dc0:5fbfb42859 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f4904bf88d"></a>
- <a id="s-4bf8a4913a"></a>`distribution`: `riverhog-client`
- <a id="s-0878a8049f"></a>`module`: `riverhog_client`
- <a id="s-7a90949f6e"></a>`name`: `get_collection_file_provenance`
- <a id="s-ccf6cf7b58"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-8b2656b18a"></a>`unit`: `member`

### Declared structure

- <a id="s-5faf370623"></a>`kind`: `"method"`
- <a id="s-b5fd43e6b3"></a>`signature`: `"\"(self, collection_id: 'CollectionId', path: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-7b277995af"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_collection_file_provenance`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b4cc5f7d3e02d80725b31bedf9bad97bbd0fb63ce5096be420b38b391345bcb6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', path: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_collection_file_provenance",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
