# stove0_protocol.JOIN_DECLARATION_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-join-declaration-format:23d79013ec -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-82f5e93359"></a>
- <a id="s-13e493804e"></a>`distribution`: `stove0-protocol`
- <a id="s-6647d11b40"></a>`module`: `stove0_protocol`
- <a id="s-bc9bcdf9a8"></a>`name`: `JOIN_DECLARATION_FORMAT`
- <a id="s-557e06c490"></a>`unit`: `export`

### Declared structure

- <a id="s-1eb91b6bb5"></a>`kind`: `"constant"`
- <a id="s-82f038106a"></a>`value`: `"stove0-join-declaration/v1"`

## Governing policies

- <a id="pa-a91a906172"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JOIN_DECLARATION_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 51c6c635f2645a22e65e39dd1f890a3ef89f3f7b3032004b5f70a9374ac8815b -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-join-declaration/v1"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JOIN_DECLARATION_FORMAT",
  "unit": "export"
}
```

</details>
