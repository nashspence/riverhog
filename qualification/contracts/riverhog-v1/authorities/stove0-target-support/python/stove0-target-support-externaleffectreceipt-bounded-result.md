# stove0_target_support.ExternalEffectReceipt.bounded_result

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-externaleffectrecei-527b0ccb8f:1e94e66089 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-047c315c74"></a>
- <a id="s-c9027fa182"></a>`distribution`: `stove0-target-support`
- <a id="s-021686c0bf"></a>`module`: `stove0_target_support`
- <a id="s-002f79bd9d"></a>`name`: `bounded_result`
- <a id="s-5459dbd1f1"></a>`owner`: `stove0_target_support.ExternalEffectReceipt`
- <a id="s-36ad279c1d"></a>`unit`: `member`

### Declared structure

- <a id="s-a4c94f2ef8"></a>`kind`: `"method"`
- <a id="s-73516e3ec7"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ExternalEffectReceipt](stove0-target-support-externaleffectreceipt.md)

## Governing policies

- <a id="pa-142322ae9c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.ExternalEffectReceipt.bounded_result`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd8cf8f81c39643f1e9f2525467afc22bc09e62352f8f811c2a9bac7133939d2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "bounded_result",
  "owner": "stove0_target_support.ExternalEffectReceipt",
  "unit": "member"
}
```

</details>
