# stove0_protocol.OciImageId

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-ociimageid:56c54df825 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-98b208ac2e"></a>
- <a id="s-e83f19e4ab"></a>`distribution`: `stove0-protocol`
- <a id="s-365965010c"></a>`module`: `stove0_protocol`
- <a id="s-60e8488031"></a>`name`: `OciImageId`
- <a id="s-8e6549df37"></a>`unit`: `export`

### Declared structure

- <a id="s-9aee471739"></a>`kind`: `"object"`
- <a id="s-94fef89462"></a>`type`: `"typing._AnnotatedAlias"`

## Governing policies

- <a id="pa-a73a8e6085"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.OciImageId`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a98b3d3bd2278e5a536dad6ab9684f5de99bd7fb75287bff85ac1b9d9315caf0 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "OciImageId",
  "unit": "export"
}
```

</details>
