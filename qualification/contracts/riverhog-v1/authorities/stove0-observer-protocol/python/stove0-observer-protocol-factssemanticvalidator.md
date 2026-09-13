# stove0_observer_protocol.FactsSemanticValidator

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-factssemanticvalidator:7caff36fe0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ca04a28465"></a>
| Field | Shape |
|---|---|
| <a id="s-fa276e6c59"></a>`contract` | type="collections.abc._CallableGenericAlias"; additional keys=`kind` |
| <a id="s-202740213a"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-f5c6354889"></a>`module` | "stove0_observer_protocol" |
| <a id="s-0489d94948"></a>`name` | "FactsSemanticValidator" |
| <a id="s-12371b69e6"></a>`unit` | "export" |

## Governing policies

- <a id="pa-4eb75eb3ea"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.FactsSemanticValidator`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cb47acb4ad8086912bab08563aeda6a8b7b644731fd14c105ca6af820e244784 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "collections.abc._CallableGenericAlias"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "FactsSemanticValidator",
  "unit": "export"
}
```
