# riverhog_client.transform.ClaimedCollectionApi.get_collection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-3669c8a7f0:a0a1659b73 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-82086e4b21"></a>
| Field | Shape |
|---|---|
| <a id="s-37b940db61"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-c9b5d57d04"></a>`distribution` | "riverhog-client" |
| <a id="s-f06edaa1cd"></a>`module` | "riverhog_client.transform" |
| <a id="s-924cc1e8ed"></a>`name` | "get_collection" |
| <a id="s-3d343a484e"></a>`owner` | "riverhog_client.transform.ClaimedCollectionApi" |
| <a id="s-5dbb77f6a5"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedCollectionApi](riverhog-client-transform-claimedcollectionapi.md)

## Governing policies

- <a id="pa-b41c2f323c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionApi.get_collection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b0ba5669a5992cf9ed8b83aafc2d4d41d108e01416b30b45a09c99479c8019b7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "get_collection",
  "owner": "riverhog_client.transform.ClaimedCollectionApi",
  "unit": "member"
}
```
