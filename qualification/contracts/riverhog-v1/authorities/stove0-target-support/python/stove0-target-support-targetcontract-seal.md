# stove0_target_support.TargetContract.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetcontract-seal:f17e99efc4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d6c10543ea"></a>
- <a id="s-3b4084f2ef"></a>`distribution`: `stove0-target-support`
- <a id="s-7156510818"></a>`module`: `stove0_target_support`
- <a id="s-463d70cb01"></a>`name`: `seal`
- <a id="s-81df4ef2dc"></a>`owner`: `stove0_target_support.TargetContract`
- <a id="s-02013099f6"></a>`unit`: `member`

### Declared structure

- <a id="s-2cc2903188"></a>`kind`: `"classmethod"`
- <a id="s-e7f037f1f1"></a>`signature`: `"\"(cls, payload: 'TargetContractPayload') -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [TargetContract](stove0-target-support-targetcontract.md)

## Governing policies

- <a id="pa-221c51b321"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetContract.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d798a45b07bbfdd0857178baea06a3ac0a4917ddb4094f3487c2abe26e985c90 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'TargetContractPayload') -> 'TargetContract'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "seal",
  "owner": "stove0_target_support.TargetContract",
  "unit": "member"
}
```

</details>
