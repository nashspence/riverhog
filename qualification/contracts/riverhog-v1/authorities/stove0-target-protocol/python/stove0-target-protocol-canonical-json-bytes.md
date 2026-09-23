# stove0_target_protocol.canonical_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-canonical-json-bytes:b615531e57 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ff942a5cf7"></a>
- <a id="s-fb3072176f"></a>`distribution`: `stove0-target-protocol`
- <a id="s-7b322a2b7f"></a>`module`: `stove0_target_protocol`
- <a id="s-17583f94c8"></a>`name`: `canonical_json_bytes`
- <a id="s-072ce0ac23"></a>`unit`: `export`

### Declared structure

- <a id="s-dc0e776293"></a>`kind`: `"function"`
- <a id="s-29659f08fd"></a>`signature`: `"\"(value: 'object') -> 'bytes'\""`

## Governing policies

- <a id="pa-a4a1d62776"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.canonical_json_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 18493f606b8431421bd7c5defc59b2dc1c922c437ca418dfeb33fc881cbb034a -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'bytes'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_json_bytes",
  "unit": "export"
}
```

</details>
