# riverhog_client.transform.ClaimedCollectionRuntime.iter_inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-d0be6b24d9:947c30235d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3833eedddf"></a>
| Field | Shape |
|---|---|
| <a id="s-aaa3c59fc1"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-23c37be55b"></a>`distribution` | "riverhog-client" |
| <a id="s-0d5ba2d1e0"></a>`module` | "riverhog_client.transform" |
| <a id="s-ce9929f23a"></a>`name` | "iter_inventory" |
| <a id="s-f418dd5cab"></a>`owner` | "riverhog_client.transform.ClaimedCollectionRuntime" |
| <a id="s-66d7730131"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedCollectionRuntime](riverhog-client-transform-claimedcollectionruntime.md)

## Governing policies

- <a id="pa-6d9c8adde7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionRuntime.iter_inventory`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 518e6ef2307246766822ddc8c933d868225ef476962135296f780f848a8eabdf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "'(self)'"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "iter_inventory",
  "owner": "riverhog_client.transform.ClaimedCollectionRuntime",
  "unit": "member"
}
```
