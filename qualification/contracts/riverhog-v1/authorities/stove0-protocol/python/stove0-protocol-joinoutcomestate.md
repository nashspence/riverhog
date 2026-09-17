# stove0_protocol.JoinOutcomeState

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinoutcomestate:0fbf253cd0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ecfa440207"></a>
- <a id="s-ebb962b183"></a>`distribution`: `stove0-protocol`
- <a id="s-476ffd1456"></a>`module`: `stove0_protocol`
- <a id="s-5e01193b75"></a>`name`: `JoinOutcomeState`
- <a id="s-95222e7641"></a>`unit`: `export`

### Declared structure

- <a id="s-2b7f2b3ec5"></a>`kind`: `"object"`
- <a id="s-2b89514dd1"></a>`type`: `"typing._LiteralGenericAlias"`

## Governing policies

- <a id="pa-9c1624d87d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JoinOutcomeState`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 86c9debf4c0927c2b89ba60321c1a15cc9524c0f8caf1d5a9164d75dd4129ff6 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JoinOutcomeState",
  "unit": "export"
}
```

</details>
