# stove0_target_protocol.TargetPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetplan:413a256b8f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0f0f01d724"></a>
- <a id="s-ce369fe8ea"></a>`distribution`: `stove0-target-protocol`
- <a id="s-caeb64852e"></a>`module`: `stove0_target_protocol`
- <a id="s-b33525b3ee"></a>`name`: `TargetPlan`
- <a id="s-67658fb177"></a>`unit`: `export`

### Declared structure

- <a id="s-b79df20fc3"></a>`kind`: `"object"`
- <a id="s-47e99b7ea4"></a>`type`: `"typing._AnnotatedAlias"`

## Governing policies

- <a id="pa-2c1e1425bb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetPlan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4755ebefd7dfda9c88abfe2e40ee921f3a52e998b36a4ee930ec8bc15872340c -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetPlan",
  "unit": "export"
}
```

</details>
