# stove0_target_support.AcceptedTargetJob.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-acceptedtargetjob-v-033e6b2cdb:139dc3a255 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-17b38fc147"></a>
- <a id="s-11a94b6eda"></a>`distribution`: `stove0-target-support`
- <a id="s-0b425d9fab"></a>`module`: `stove0_target_support`
- <a id="s-cb27ae37cf"></a>`name`: `verify_digest`
- <a id="s-462a49d15f"></a>`owner`: `stove0_target_support.AcceptedTargetJob`
- <a id="s-0cd62286e6"></a>`unit`: `member`

### Declared structure

- <a id="s-5d62e009ad"></a>`kind`: `"method"`
- <a id="s-6bb1b0f480"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_support.AcceptedTargetJob](stove0-target-support-acceptedtargetjob.md)

## Governing policies

- <a id="pa-d987d3c3e4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.AcceptedTargetJob.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0573e27fe98aad67d7f0474a1a4d6c1fd3a72c251cd0404038535d502d209515 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "verify_digest",
  "owner": "stove0_target_support.AcceptedTargetJob",
  "unit": "member"
}
```
