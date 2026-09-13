# riverhog_client.transform.ClaimedCollectionReader.iter_inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-e67ce75798:0987ec5528 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-de82d995f0"></a>
| Field | Shape |
|---|---|
| <a id="s-9e3361eed9"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-e53229ba7e"></a>`distribution` | "riverhog-client" |
| <a id="s-c34dbf840e"></a>`module` | "riverhog_client.transform" |
| <a id="s-8c8c6a370f"></a>`name` | "iter_inventory" |
| <a id="s-0e3dec6a40"></a>`owner` | "riverhog_client.transform.ClaimedCollectionReader" |
| <a id="s-e035252645"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedCollectionReader](riverhog-client-transform-claimedcollectionreader.md)

## Governing policies

- <a id="pa-57167fbe84"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionReader.iter_inventory`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 95c7b68e1153b1d94c2d8d156d91167d3b69cd117a9d1011969dd087f3f5c4b7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, include_control: 'bool' = False) -> 'Iterator[ClaimedArtifact]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "iter_inventory",
  "owner": "riverhog_client.transform.ClaimedCollectionReader",
  "unit": "member"
}
```
