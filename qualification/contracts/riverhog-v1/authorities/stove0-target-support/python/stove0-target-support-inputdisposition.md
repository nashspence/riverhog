# stove0_target_support.InputDisposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-inputdisposition:c70dfb7974 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e814824c55"></a>
- <a id="s-926fce4a0e"></a>`distribution`: `stove0-target-support`
- <a id="s-c2acf65591"></a>`module`: `stove0_target_support`
- <a id="s-56ff2f95d5"></a>`name`: `InputDisposition`
- <a id="s-b33af9ab4a"></a>`unit`: `export`

### Declared structure

- <a id="s-72f821b6e6"></a>`kind`: `"object"`
- <a id="s-f1f8e1575c"></a>`type`: `"typing._LiteralGenericAlias"`

## Governing policies

- <a id="pa-e0fe27a3ed"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.InputDisposition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 05fe9f986125ecae227e688ebca297a08e519079547ad6cd7550585c1e3e2142 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "InputDisposition",
  "unit": "export"
}
```

</details>
