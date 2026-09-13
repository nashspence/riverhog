# riverhog_client.transform.ClaimedCollectionRuntimeRegistry.discard

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-f2078919ea:ecffc117a3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-51df3578b8"></a>
| Field | Shape |
|---|---|
| <a id="s-22041069c0"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-55268b980a"></a>`distribution` | "riverhog-client" |
| <a id="s-1913ab0c8d"></a>`module` | "riverhog_client.transform" |
| <a id="s-674a827d78"></a>`name` | "discard" |
| <a id="s-5a8e623ac0"></a>`owner` | "riverhog_client.transform.ClaimedCollectionRuntimeRegistry" |
| <a id="s-4e43688892"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedCollectionRuntimeRegistry](riverhog-client-transform-claimedcollectionruntimeregistry.md)

## Governing policies

- <a id="pa-ddec1fe7b3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionRuntimeRegistry.discard`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f50fc22764f4ad68e39300d5b1878770a1ed2e7284091f8e8b8f159e68510db -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "discard",
  "owner": "riverhog_client.transform.ClaimedCollectionRuntimeRegistry",
  "unit": "member"
}
```
