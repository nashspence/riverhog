# stove0_target_support.ExternalEffectReceipt.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-externaleffectrecei-079faa56f0:39c10bde10 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3a0d71cd3e"></a>
- <a id="s-a8d1fc8a2d"></a>`distribution`: `stove0-target-support`
- <a id="s-f9c7726e95"></a>`module`: `stove0_target_support`
- <a id="s-60ee803a00"></a>`name`: `verify_digest`
- <a id="s-d674dc1091"></a>`owner`: `stove0_target_support.ExternalEffectReceipt`
- <a id="s-74ae27017d"></a>`unit`: `member`

### Declared structure

- <a id="s-009eafe93a"></a>`kind`: `"method"`
- <a id="s-f7b9c87eb2"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ExternalEffectReceipt](stove0-target-support-externaleffectreceipt.md)

## Governing policies

- <a id="pa-12fb923d43"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.ExternalEffectReceipt.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 50ad6d63b2708a054e265e3b1bdad274a20d0605eaa0cdcf20f9bd5da9dbb9e0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "verify_digest",
  "owner": "stove0_target_support.ExternalEffectReceipt",
  "unit": "member"
}
```
