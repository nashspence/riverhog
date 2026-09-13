# stove0_observer_protocol.SemanticValidatorProvider

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticvalidatorprovider:72c38baf94 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d5c91df008"></a>
| Field | Shape |
|---|---|
| <a id="s-698ac2f1ab"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-db786e96f3"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-7e6c89cfb8"></a>`module` | "stove0_observer_protocol" |
| <a id="s-716fb6ecff"></a>`name` | "SemanticValidatorProvider" |
| <a id="s-3a0ace2586"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.SemanticValidatorProvider.resolve](stove0-observer-protocol-semanticvalidatorprovider-resolve.md)

## Governing policies

- <a id="pa-1884b4ce5d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticValidatorProvider`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 543e8ce13a6e305928178b93ed504e097f2002050a6dde471a68ae145041b3ea -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "SemanticValidatorProvider",
  "unit": "export"
}
```
