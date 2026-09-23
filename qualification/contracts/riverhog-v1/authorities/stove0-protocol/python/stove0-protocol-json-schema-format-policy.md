# stove0_protocol.JSON_SCHEMA_FORMAT_POLICY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-json-schema-format-policy:c679d99b42 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4917c12c7"></a>
- <a id="s-5fb5e45ed4"></a>`distribution`: `stove0-protocol`
- <a id="s-e674995f82"></a>`module`: `stove0_protocol`
- <a id="s-aed59dceed"></a>`name`: `JSON_SCHEMA_FORMAT_POLICY`
- <a id="s-4172ab457f"></a>`unit`: `export`

### Declared structure

- <a id="s-93e84d1998"></a>`kind`: `"constant"`
- <a id="s-6c6b9ad60e"></a>`value`: `"annotation-only"`

## Governing policies

- <a id="pa-053a38f40b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JSON_SCHEMA_FORMAT_POLICY`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a70490d9c15c862ac55c5f4030fb75844bdb792eb6717f96dc4a29f7215f35b2 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "annotation-only"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JSON_SCHEMA_FORMAT_POLICY",
  "unit": "export"
}
```

</details>
