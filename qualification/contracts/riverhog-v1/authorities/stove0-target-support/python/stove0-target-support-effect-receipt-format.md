# stove0_target_support.EFFECT_RECEIPT_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-effect-receipt-format:3feb4c219e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-106eef1755"></a>
- <a id="s-7518f99e3b"></a>`distribution`: `stove0-target-support`
- <a id="s-f62bb454b5"></a>`module`: `stove0_target_support`
- <a id="s-f1c71a6c87"></a>`name`: `EFFECT_RECEIPT_FORMAT`
- <a id="s-9bf9ac53ea"></a>`unit`: `export`

### Declared structure

- <a id="s-7cf6f8e513"></a>`kind`: `"constant"`
- <a id="s-d258c57bc9"></a>`value`: `"stove0-external-effect-receipt/v1"`

## Governing policies

- <a id="pa-5a197d1ae0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.EFFECT_RECEIPT_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e57ec0183bc9229c7a77ae030a1ac183a7eb50183be51f57540b9e05b66e6232 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-external-effect-receipt/v1"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "EFFECT_RECEIPT_FORMAT",
  "unit": "export"
}
```

</details>
