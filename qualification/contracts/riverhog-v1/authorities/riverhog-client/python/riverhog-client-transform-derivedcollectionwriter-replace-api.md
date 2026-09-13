# riverhog_client.transform.DerivedCollectionWriter.replace_api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-derivedcollecti-2a7a37675f:ee6ad51ba4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dde0313542"></a>
| Field | Shape |
|---|---|
| <a id="s-9c91d08809"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-c6e22f6ef9"></a>`distribution` | "riverhog-client" |
| <a id="s-a4bd3004bc"></a>`module` | "riverhog_client.transform" |
| <a id="s-3a6e81aa2d"></a>`name` | "replace_api" |
| <a id="s-44b082d0e3"></a>`owner` | "riverhog_client.transform.DerivedCollectionWriter" |
| <a id="s-87c4e47543"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.DerivedCollectionWriter](riverhog-client-transform-derivedcollectionwriter.md)

## Governing policies

- <a id="pa-6be7deddf2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.DerivedCollectionWriter.replace_api`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e80b98838d866b1b864bf53b1e92a7c57f2d9c2ab65fa442ee5a87c3b6d0ba52 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, api: 'Any') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "replace_api",
  "owner": "riverhog_client.transform.DerivedCollectionWriter",
  "unit": "member"
}
```
