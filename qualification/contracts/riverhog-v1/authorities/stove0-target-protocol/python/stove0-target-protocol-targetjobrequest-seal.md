# stove0_target_protocol.TargetJobRequest.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetjobrequest-seal:cd1c3b2328 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-09886340b1"></a>
- <a id="s-4c41564a0e"></a>`distribution`: `stove0-target-protocol`
- <a id="s-8e4591e3df"></a>`module`: `stove0_target_protocol`
- <a id="s-8f74bc8600"></a>`name`: `seal`
- <a id="s-e84dc57540"></a>`owner`: `stove0_target_protocol.TargetJobRequest`
- <a id="s-e7c1029840"></a>`unit`: `member`

### Declared structure

- <a id="s-879ce95e35"></a>`kind`: `"classmethod"`
- <a id="s-c6c8632f2b"></a>`signature`: `"\"(cls, declaration: 'TargetJobDeclaration', runtime: 'TargetRuntimeAuthority', callback_access: 'TargetCallbackAccess') -> 'TargetJobRequest'\""`

## Maintained corroboration

### Related interface records

- [TargetJobRequest](stove0-target-protocol-targetjobrequest.md)

## Governing policies

- <a id="pa-3e0a7ca6c3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetJobRequest.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb0d10eaafb587b330080f96f55eeca9ddebde666d0a9d311a5e5ea1fb2b0ee2 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, declaration: 'TargetJobDeclaration', runtime: 'TargetRuntimeAuthority', callback_access: 'TargetCallbackAccess') -> 'TargetJobRequest'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal",
  "owner": "stove0_target_protocol.TargetJobRequest",
  "unit": "member"
}
```

</details>
