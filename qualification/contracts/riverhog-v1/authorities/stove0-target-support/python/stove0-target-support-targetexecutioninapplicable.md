# stove0_target_support.TargetExecutionInapplicable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetexecutioninapplicable:081de8f615 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d1caa61ee"></a>
- <a id="s-747c48fe1d"></a>`distribution`: `stove0-target-support`
- <a id="s-8399cfb91c"></a>`module`: `stove0_target_support`
- <a id="s-c94e7ac30d"></a>`name`: `TargetExecutionInapplicable`
- <a id="s-f927eb3eba"></a>`unit`: `export`

### Declared structure

- <a id="s-9034a175e9"></a>`kind`: `"class"`
- <a id="s-6b0fcb380f"></a>`signature`: `"\"(code: 'str', message: 'str') -> 'None'\""`

## Governing policies

- <a id="pa-86788c49be"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetExecutionInapplicable`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 48eba60e9e935f347823d7bd70adf566ee07fa255ab4dcdd7c47edc81aef2429 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(code: 'str', message: 'str') -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetExecutionInapplicable",
  "unit": "export"
}
```
