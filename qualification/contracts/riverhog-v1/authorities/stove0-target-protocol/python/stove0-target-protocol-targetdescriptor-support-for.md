# stove0_target_protocol.TargetDescriptor.support_for

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetdescriptor-support-for:0ea3111dc9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9cc0cf5d6a"></a>
- <a id="s-82e31d1e07"></a>`distribution`: `stove0-target-protocol`
- <a id="s-ff367ed77b"></a>`module`: `stove0_target_protocol`
- <a id="s-4423b07e9a"></a>`name`: `support_for`
- <a id="s-ed47b71be8"></a>`owner`: `stove0_target_protocol.TargetDescriptor`
- <a id="s-e02c4a539a"></a>`unit`: `member`

### Declared structure

- <a id="s-6f2807aaf6"></a>`kind`: `"method"`
- <a id="s-9a03765d0a"></a>`signature`: `"\"(self, operation_id: 'str') -> 'TargetOperationSupport'\""`

## Maintained corroboration

### Related interface records

- [TargetDescriptor](stove0-target-protocol-targetdescriptor.md)

## Governing policies

- <a id="pa-931a7f3d5b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetDescriptor.support_for`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5e1099df681b17e56d219e7b5fab5948cf4238af8642a18b4c8e52f9c73d7d82 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, operation_id: 'str') -> 'TargetOperationSupport'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "support_for",
  "owner": "stove0_target_protocol.TargetDescriptor",
  "unit": "member"
}
```

</details>
