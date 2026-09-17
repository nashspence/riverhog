# stove0_protocol.Stove0ProtocolModel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-stove0protocolmodel:47eee20483 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-046541df97"></a>
- <a id="s-bda88f422e"></a>`distribution`: `stove0-protocol`
- <a id="s-7ce36bc967"></a>`module`: `stove0_protocol`
- <a id="s-4dadb4ccc6"></a>`name`: `Stove0ProtocolModel`
- <a id="s-483424cbf1"></a>`unit`: `export`

### Declared structure

- <a id="s-2e88ed59c3"></a>`kind`: `"class"`
- <a id="s-5ed43baf48"></a>`signature`: `"'() -> None'"`

#### Validated model schema

<a id="s-8069c002cc"></a>

- <a id="s-8179df7924"></a>`type`: `"object"`
- <a id="s-2ed3c9db08"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|

## Governing policies

- <a id="pa-e5f21081fa"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.Stove0ProtocolModel`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89a98512ce0d81df8c1f3fc14d05c476ced71a0863a6294a896fe9b6412c39c3 -->

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
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "Stove0ProtocolModel",
  "unit": "export"
}
```

</details>
