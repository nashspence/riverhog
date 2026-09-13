# stove0_target_support.TargetClient.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetclient-contract:699de496dc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3d2afe2299"></a>
| Field | Shape |
|---|---|
| <a id="s-e9fdf1b288"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d2034c5eb3"></a>`distribution` | "stove0-target-support" |
| <a id="s-eef7c92f86"></a>`module` | "stove0_target_support" |
| <a id="s-e8b85360dd"></a>`name` | "contract" |
| <a id="s-9c2f670e30"></a>`owner` | "stove0_target_support.TargetClient" |
| <a id="s-32f514f372"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetClient](stove0-target-support-targetclient.md)

## Governing policies

- <a id="pa-e79e81e222"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetClient.contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9c8158d6d88ddb55d9d02b67d42e3cb4279f7f1c16d929bd7bcd3f87aa7e0e32 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetContract'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "contract",
  "owner": "stove0_target_support.TargetClient",
  "unit": "member"
}
```
