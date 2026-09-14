# riverhog_client.ApiClient.get_collection_provenance_verification

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-collection-bfdae0b10f:01efeeb145 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2599365818"></a>
- <a id="s-54e45381e1"></a>`distribution`: `riverhog-client`
- <a id="s-c6b38abf52"></a>`module`: `riverhog_client`
- <a id="s-d9d4c47c0c"></a>`name`: `get_collection_provenance_verification`
- <a id="s-c03406eafb"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-870dab3e5a"></a>`unit`: `member`

### Declared structure

- <a id="s-d1b3d6c759"></a>`kind`: `"method"`
- <a id="s-60eab60e62"></a>`signature`: `"\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-6b8be81246"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_collection_provenance_verification`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 50ec68d6f6daf7cb9cb68d78f236f69af76197490ecf255ce692ca421a9216ac -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_collection_provenance_verification",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
