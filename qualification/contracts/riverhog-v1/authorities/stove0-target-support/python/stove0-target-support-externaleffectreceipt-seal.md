# stove0_target_support.ExternalEffectReceipt.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-externaleffectreceipt-seal:0ee5b12c50 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cb86189908"></a>
- <a id="s-4dc4916294"></a>`distribution`: `stove0-target-support`
- <a id="s-0f11163d5e"></a>`module`: `stove0_target_support`
- <a id="s-61fdbee862"></a>`name`: `seal`
- <a id="s-da4ceef95f"></a>`owner`: `stove0_target_support.ExternalEffectReceipt`
- <a id="s-9217c41a6e"></a>`unit`: `member`

### Declared structure

- <a id="s-631bc11454"></a>`kind`: `"classmethod"`
- <a id="s-6d67a6baa4"></a>`signature`: `"\"(cls, payload: 'ExternalEffectReceiptPayload') -> 'ExternalEffectReceipt'\""`

## Maintained corroboration

### Related interface records

- [ExternalEffectReceipt](stove0-target-support-externaleffectreceipt.md)

## Governing policies

- <a id="pa-20b11881a1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.ExternalEffectReceipt.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b5f29c2606271447eae8aee6aebf51d18e1a019f544caa2a80ccbb21c0d4b1d -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'ExternalEffectReceiptPayload') -> 'ExternalEffectReceipt'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "seal",
  "owner": "stove0_target_support.ExternalEffectReceipt",
  "unit": "member"
}
```

</details>
