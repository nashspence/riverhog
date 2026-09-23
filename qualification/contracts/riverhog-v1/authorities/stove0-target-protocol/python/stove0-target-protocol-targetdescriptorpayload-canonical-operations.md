# stove0_target_protocol.TargetDescriptorPayload.canonical_operations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetdescriptorpa-fa9c4f1432:399ab5f9f3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-41f6b9939c"></a>
- <a id="s-cc80af3d32"></a>`distribution`: `stove0-target-protocol`
- <a id="s-7f250c1dc5"></a>`module`: `stove0_target_protocol`
- <a id="s-ae2e9bd600"></a>`name`: `canonical_operations`
- <a id="s-6ee023c4db"></a>`owner`: `stove0_target_protocol.TargetDescriptorPayload`
- <a id="s-af8d9d06a5"></a>`unit`: `member`

### Declared structure

- <a id="s-562dcad611"></a>`kind`: `"classmethod"`
- <a id="s-0a66830189"></a>`signature`: `"\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""`

## Maintained corroboration

### Related interface records

- [TargetDescriptorPayload](stove0-target-protocol-targetdescriptorpayload.md)

## Governing policies

- <a id="pa-3693f14de9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetDescriptorPayload.canonical_operations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8394a48eace2f19792e38147f20cd7f8eb3d59c4616d0e86a97e76c16b76f54c -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_operations",
  "owner": "stove0_target_protocol.TargetDescriptorPayload",
  "unit": "member"
}
```

</details>
