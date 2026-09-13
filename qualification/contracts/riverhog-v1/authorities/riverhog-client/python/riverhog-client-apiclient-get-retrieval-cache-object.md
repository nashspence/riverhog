# riverhog_client.ApiClient.get_retrieval_cache_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-retrieval-cache-object:cebbc4d0ff -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-273555662f"></a>
| Field | Shape |
|---|---|
| <a id="s-9faadae456"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-7b6aebfd5d"></a>`distribution` | "riverhog-client" |
| <a id="s-d3cc4d6d03"></a>`module` | "riverhog_client" |
| <a id="s-f3ad74850f"></a>`name` | "get_retrieval_cache_object" |
| <a id="s-dc3a06c29b"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-e021254b34"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-852bc2b671"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_retrieval_cache_object`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b25188afe034cd0e57392e174d0c6f681611ef2c44925f319dd08e1c4abcc79f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', source_store: 'ArchiveStoreName', object_id: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_retrieval_cache_object",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
