# stove0_protocol.JSON_SCHEMA_FORMAT_POLICY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-json-schema-format-policy:c679d99b42 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4917c12c7"></a>
| Field | Shape |
|---|---|
| <a id="s-48f4a56ff9"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-5fb5e45ed4"></a>`distribution` | "stove0-protocol" |
| <a id="s-e674995f82"></a>`module` | "stove0_protocol" |
| <a id="s-aed59dceed"></a>`name` | "JSON_SCHEMA_FORMAT_POLICY" |
| <a id="s-4172ab457f"></a>`unit` | "export" |

## Governing policies

- <a id="pa-053a38f40b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JSON_SCHEMA_FORMAT_POLICY`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a70490d9c15c862ac55c5f4030fb75844bdb792eb6717f96dc4a29f7215f35b2 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "annotation-only"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JSON_SCHEMA_FORMAT_POLICY",
  "unit": "export"
}
```
