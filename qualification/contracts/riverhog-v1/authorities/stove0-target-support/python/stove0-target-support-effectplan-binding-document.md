# stove0_target_support.EffectPlan.binding_document

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-effectplan-binding-document:63eea5d2f5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3315fddfbb"></a>
- <a id="s-f3b7219b3b"></a>`distribution`: `stove0-target-support`
- <a id="s-134e2c41d8"></a>`module`: `stove0_target_support`
- <a id="s-d4429f11b4"></a>`name`: `binding_document`
- <a id="s-f2367c1e59"></a>`owner`: `stove0_target_support.EffectPlan`
- <a id="s-37371eb4ff"></a>`unit`: `member`

### Declared structure

- <a id="s-7226b9daa9"></a>`kind`: `"method"`
- <a id="s-6d4ad1e6a4"></a>`signature`: `"\"(self) -> 'dict[str, JsonValue]'\""`

## Maintained corroboration

### Related interface records

- [EffectPlan](stove0-target-support-effectplan.md)

## Governing policies

- <a id="pa-7ea5c4efa2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.EffectPlan.binding_document`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0f04b22c158fe93bf592dc1a7857b14f66d201cb78291b21ac0c4faa5f29abd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, JsonValue]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "binding_document",
  "owner": "stove0_target_support.EffectPlan",
  "unit": "member"
}
```

</details>
