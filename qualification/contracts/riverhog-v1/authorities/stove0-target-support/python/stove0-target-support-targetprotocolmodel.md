# stove0_target_support.TargetProtocolModel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetprotocolmodel:f650909d59 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-deccbcd5af"></a>
- <a id="s-5ca1635ebf"></a>`distribution`: `stove0-target-support`
- <a id="s-b730c98ed4"></a>`module`: `stove0_target_support`
- <a id="s-bf4669648d"></a>`name`: `TargetProtocolModel`
- <a id="s-c0ac5d6432"></a>`unit`: `export`

### Declared structure

- <a id="s-0ea7307c53"></a>`kind`: `"class"`
- <a id="s-229f8eb0b9"></a>`signature`: `"'() -> None'"`

#### Validated model schema

<a id="s-4709ad4014"></a>
- <a id="s-8437a3c10d"></a>`title`: TargetProtocolModel
- <a id="s-b72f5a41c3"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|

## Governing policies

- <a id="pa-f4c5ce0bb1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetProtocolModel`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 60e9fa3e6791bf28a2cf02b0e3633aadad2fbad4adbb4f23491cb60652ef12a8 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {},
      "title": "TargetProtocolModel",
      "type": "object"
    },
    "signature": "'() -> None'"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetProtocolModel",
  "unit": "export"
}
```
