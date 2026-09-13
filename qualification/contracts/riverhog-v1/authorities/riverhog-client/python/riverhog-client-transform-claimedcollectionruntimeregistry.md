# riverhog_client.transform.ClaimedCollectionRuntimeRegistry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-c2d29a43cb:17b9059a84 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6679cc5990"></a>
| Field | Shape |
|---|---|
| <a id="s-45abfc5864"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-2fb91860c8"></a>`distribution` | "riverhog-client" |
| <a id="s-75cb2041d0"></a>`module` | "riverhog_client.transform" |
| <a id="s-1de6005606"></a>`name` | "ClaimedCollectionRuntimeRegistry" |
| <a id="s-35ff505068"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedCollectionRuntimeRegistry.bind](riverhog-client-transform-claimedcollectionruntimeregistry-bind.md)
- [riverhog_client.transform.ClaimedCollectionRuntimeRegistry.discard](riverhog-client-transform-claimedcollectionruntimeregistry-discard.md)
- [riverhog_client.transform.ClaimedCollectionRuntimeRegistry.refresh](riverhog-client-transform-claimedcollectionruntimeregistry-refresh.md)

## Governing policies

- <a id="pa-80511ba0b2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionRuntimeRegistry`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b8735be368267fae4926b34f3dfc56fc1a0b9900e9fb07d867202b3213624fb7 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"() -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "ClaimedCollectionRuntimeRegistry",
  "unit": "export"
}
```
