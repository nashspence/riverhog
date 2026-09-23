# stove0_target_protocol.InputDisposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-inputdisposition:bafd9fd828 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5e13fa1198"></a>
- <a id="s-b573add80a"></a>`distribution`: `stove0-target-protocol`
- <a id="s-13439d00bf"></a>`module`: `stove0_target_protocol`
- <a id="s-f50c684036"></a>`name`: `InputDisposition`
- <a id="s-184df55a70"></a>`unit`: `export`

### Declared structure

- <a id="s-86fb7a888a"></a>`kind`: `"object"`
- <a id="s-6e95e63f03"></a>`type`: `"typing._LiteralGenericAlias"`

## Governing policies

- <a id="pa-1f42491aa9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.InputDisposition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82242607bb8bccd59e6f1cc48383990bdbd8d03c34d786ff6a39b79b127570cb -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "InputDisposition",
  "unit": "export"
}
```

</details>
