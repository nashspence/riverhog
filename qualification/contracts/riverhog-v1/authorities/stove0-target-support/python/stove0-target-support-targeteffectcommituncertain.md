# stove0_target_support.TargetEffectCommitUncertain

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targeteffectcommituncertain:92472250cb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f173573d75"></a>
| Field | Shape |
|---|---|
| <a id="s-48c21b7380"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-166df5de1d"></a>`distribution` | "stove0-target-support" |
| <a id="s-cb143dd774"></a>`module` | "stove0_target_support" |
| <a id="s-dce771c116"></a>`name` | "TargetEffectCommitUncertain" |
| <a id="s-f653589fc3"></a>`unit` | "export" |

## Governing policies

- <a id="pa-782af4330a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetEffectCommitUncertain`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7cf464c48a9053a1e9f6985b14b646673d004da0791295ddea089077af1f84b7 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "unavailable"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetEffectCommitUncertain",
  "unit": "export"
}
```
