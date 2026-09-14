# stove0_target_support.ARTIFACT_ID_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-artifact-id-pattern:43f154255d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d1475d3370"></a>
- <a id="s-8114638b79"></a>`distribution`: `stove0-target-support`
- <a id="s-563b854c66"></a>`module`: `stove0_target_support`
- <a id="s-0c59cc7348"></a>`name`: `ARTIFACT_ID_PATTERN`
- <a id="s-d8eafe250f"></a>`unit`: `export`

### Declared structure

- <a id="s-84e00894fa"></a>`kind`: `"constant"`
- <a id="s-4fcf67afa8"></a>`value`: `"^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"`

## Governing policies

- <a id="pa-42d9fceda8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.ARTIFACT_ID_PATTERN`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eaebb65f4d623834bb595575e183e7adfd177a49b42fc016a5b77b6fe6101eab -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "ARTIFACT_ID_PATTERN",
  "unit": "export"
}
```
