# stove0_target_support.target_schema_bundle

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-target-schema-bundle:2198c84916 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-099142a850"></a>
- <a id="s-10614f5f0e"></a>`distribution`: `stove0-target-support`
- <a id="s-c06a377ec5"></a>`module`: `stove0_target_support`
- <a id="s-e7756dc83f"></a>`name`: `target_schema_bundle`
- <a id="s-3c074b230b"></a>`unit`: `export`

### Declared structure

- <a id="s-b18a9f9446"></a>`kind`: `"function"`
- <a id="s-7b0b19284d"></a>`signature`: `"\"() -> 'dict[str, Any]'\""`

## Governing policies

- <a id="pa-78a4fe6911"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.target_schema_bundle`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f0bc7d3c591a0a621257c2e618e6cd79a9c8cdd6e39a21c16cd7af64a923d23f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'dict[str, Any]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "target_schema_bundle",
  "unit": "export"
}
```
