# stove0_target_support.EFFECT_TARGET_PROTOCOL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-effect-target-protocol:c42fe14577 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0661a699e6"></a>
- <a id="s-247f54ecde"></a>`distribution`: `stove0-target-support`
- <a id="s-eed3bb3e98"></a>`module`: `stove0_target_support`
- <a id="s-104981d5c3"></a>`name`: `EFFECT_TARGET_PROTOCOL`
- <a id="s-103af778b6"></a>`unit`: `export`

### Declared structure

- <a id="s-dcff3ff270"></a>`kind`: `"constant"`
- <a id="s-1f11c030a0"></a>`value`: `"stove0-effect-target/v1"`

## Governing policies

- <a id="pa-8499f6f62b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.EFFECT_TARGET_PROTOCOL`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8778f8bf84dcdbe6994bed8cefbb5f7f5c9751e12b3b3fe05edfecee45d470e6 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-effect-target/v1"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "EFFECT_TARGET_PROTOCOL",
  "unit": "export"
}
```

</details>
