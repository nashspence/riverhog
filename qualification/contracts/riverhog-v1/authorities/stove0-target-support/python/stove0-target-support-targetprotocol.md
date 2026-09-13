# stove0_target_support.TargetProtocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetprotocol:440f542e49 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3b180111dd"></a>
| Field | Shape |
|---|---|
| <a id="s-914c0ba6e4"></a>`contract` | type="typing._LiteralGenericAlias"; additional keys=`kind` |
| <a id="s-09e84e3bee"></a>`distribution` | "stove0-target-support" |
| <a id="s-1303b1a64e"></a>`module` | "stove0_target_support" |
| <a id="s-746684db4c"></a>`name` | "TargetProtocol" |
| <a id="s-2281332ec5"></a>`unit` | "export" |

## Governing policies

- <a id="pa-99840b16dd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetProtocol`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7d53258c0aa3cbc6588be45a134c6ddfb1513fe57ede3284318db40a8e4fcde7 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetProtocol",
  "unit": "export"
}
```
