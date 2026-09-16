# stove0_target_protocol.TargetProtocolModel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetprotocolmodel:09578bd330 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c609296245"></a>
- <a id="s-2923974e6c"></a>`distribution`: `stove0-target-protocol`
- <a id="s-bf3b93adaa"></a>`module`: `stove0_target_protocol`
- <a id="s-0d704363d3"></a>`name`: `TargetProtocolModel`
- <a id="s-ac4b934a57"></a>`unit`: `export`

### Declared structure

- <a id="s-aab5424c2c"></a>`kind`: `"class"`
- <a id="s-e686acb949"></a>`signature`: `"'() -> None'"`

#### Validated model schema

<a id="s-17ef776d55"></a>

- <a id="s-fd946aa206"></a>`type`: `"object"`
- <a id="s-6f1d90f1c2"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|

## Governing policies

- <a id="pa-e19d717ad8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProtocolModel`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39d861f2d9837d8c2ef446a25f084774bbc2ea5a4e51aa8f888a960b5cf56ee3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {},
      "type": "object"
    },
    "signature": "'() -> None'"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetProtocolModel",
  "unit": "export"
}
```

</details>
