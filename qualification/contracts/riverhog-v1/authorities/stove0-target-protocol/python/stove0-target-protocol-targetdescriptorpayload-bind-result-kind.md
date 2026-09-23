# stove0_target_protocol.TargetDescriptorPayload.bind_result_kind

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetdescriptorpa-0681c21c74:62fae7fe8d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-41adbbf59a"></a>
- <a id="s-a22d256353"></a>`distribution`: `stove0-target-protocol`
- <a id="s-f74d41dd28"></a>`module`: `stove0_target_protocol`
- <a id="s-daedc9fc8c"></a>`name`: `bind_result_kind`
- <a id="s-cf823e437f"></a>`owner`: `stove0_target_protocol.TargetDescriptorPayload`
- <a id="s-c8ab9a4a36"></a>`unit`: `member`

### Declared structure

- <a id="s-3ca755d405"></a>`kind`: `"method"`
- <a id="s-fb698fc033"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TargetDescriptorPayload](stove0-target-protocol-targetdescriptorpayload.md)

## Governing policies

- <a id="pa-3c5a38801b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetDescriptorPayload.bind_result_kind`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 442d83a5aca517fe9e2708bd6c02f9db55b82a66555bb9d0e2a6c0be7a09d93b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "bind_result_kind",
  "owner": "stove0_target_protocol.TargetDescriptorPayload",
  "unit": "member"
}
```

</details>
