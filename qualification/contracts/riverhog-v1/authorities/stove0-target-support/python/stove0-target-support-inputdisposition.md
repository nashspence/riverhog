# stove0_target_support.InputDisposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-inputdisposition:c70dfb7974 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e814824c55"></a>
| Field | Shape |
|---|---|
| <a id="s-9ef11db70d"></a>`contract` | type="typing._LiteralGenericAlias"; additional keys=`kind` |
| <a id="s-926fce4a0e"></a>`distribution` | "stove0-target-support" |
| <a id="s-c2acf65591"></a>`module` | "stove0_target_support" |
| <a id="s-56ff2f95d5"></a>`name` | "InputDisposition" |
| <a id="s-b33af9ab4a"></a>`unit` | "export" |

## Governing policies

- <a id="pa-e0fe27a3ed"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.InputDisposition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 05fe9f986125ecae227e688ebca297a08e519079547ad6cd7550585c1e3e2142 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "InputDisposition",
  "unit": "export"
}
```
