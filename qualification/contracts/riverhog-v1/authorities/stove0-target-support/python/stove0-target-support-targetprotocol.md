# stove0_target_support.TargetProtocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetprotocol:440f542e49 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3b180111dd"></a>
- <a id="s-09e84e3bee"></a>`distribution`: `stove0-target-support`
- <a id="s-1303b1a64e"></a>`module`: `stove0_target_support`
- <a id="s-746684db4c"></a>`name`: `TargetProtocol`
- <a id="s-2281332ec5"></a>`unit`: `export`

### Declared structure

- <a id="s-cdc51c0c4b"></a>`kind`: `"object"`
- <a id="s-fb19291591"></a>`type`: `"typing._LiteralGenericAlias"`

## Governing policies

- <a id="pa-99840b16dd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetProtocol`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7d53258c0aa3cbc6588be45a134c6ddfb1513fe57ede3284318db40a8e4fcde7 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetProtocol",
  "unit": "export"
}
```

</details>
