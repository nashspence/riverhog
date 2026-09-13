# riverhog_client.transform.ClaimedCollectionRuntimeRegistry.bind

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-16ba6fee9f:f19d3891a4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7b95fac5b8"></a>
| Field | Shape |
|---|---|
| <a id="s-4d2b74a309"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-90cd4eb58f"></a>`distribution` | "riverhog-client" |
| <a id="s-e618e09c8e"></a>`module` | "riverhog_client.transform" |
| <a id="s-aa5d9d2124"></a>`name` | "bind" |
| <a id="s-38ee6215ad"></a>`owner` | "riverhog_client.transform.ClaimedCollectionRuntimeRegistry" |
| <a id="s-04f7f5367f"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedCollectionRuntimeRegistry](riverhog-client-transform-claimedcollectionruntimeregistry.md)

## Governing policies

- <a id="pa-12c8872088"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionRuntimeRegistry.bind`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6b6256e51de3370e9ec8f9efe01bf846c324597a4559d91e8c4e4b8dd85742f0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', runtime: 'RefreshableClaimedCollectionRuntime') -> 'Iterator[RefreshableClaimedCollectionRuntime]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "bind",
  "owner": "riverhog_client.transform.ClaimedCollectionRuntimeRegistry",
  "unit": "member"
}
```
