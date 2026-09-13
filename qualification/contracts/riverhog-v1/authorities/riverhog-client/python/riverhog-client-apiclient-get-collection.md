# riverhog_client.ApiClient.get_collection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-collection:594dd21f6c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-139481444d"></a>
| Field | Shape |
|---|---|
| <a id="s-86a614e40d"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a8c73fc8d7"></a>`distribution` | "riverhog-client" |
| <a id="s-acb6caa872"></a>`module` | "riverhog_client" |
| <a id="s-9c611be313"></a>`name` | "get_collection" |
| <a id="s-912c42f976"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-19708e57a1"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-4df2fea9b0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_collection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 63caa80c2e75165ff4cb572ece2481086c6b03a520cd6c0bcba88e696d74bf08 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_collection",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
