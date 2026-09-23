# stove0_target_support.TargetClient.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetclient-descriptor:3d6bef5cfe -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ab87bb6fdc"></a>
- <a id="s-f2f5a55ed7"></a>`distribution`: `stove0-target-support`
- <a id="s-c66d0ab687"></a>`module`: `stove0_target_support`
- <a id="s-51b1e905ff"></a>`name`: `descriptor`
- <a id="s-a06c3af33c"></a>`owner`: `stove0_target_support.TargetClient`
- <a id="s-85931686be"></a>`unit`: `member`

### Declared structure

- <a id="s-99ebcf1d7f"></a>`kind`: `"method"`
- <a id="s-6b2b473d69"></a>`signature`: `"\"(self) -> 'TargetDescriptor'\""`

## Maintained corroboration

### Related interface records

- [TargetClient](stove0-target-support-targetclient.md)

## Governing policies

- <a id="pa-005f8e0dae"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetClient.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59465f86b6ce8426b4a640deebda8745e87eebab15a8736e4b73980dc22ed6e5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetDescriptor'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "descriptor",
  "owner": "stove0_target_support.TargetClient",
  "unit": "member"
}
```

</details>
