# stove0_target_protocol.TargetDescriptor.canonical_operations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetdescriptor-c-00a16ece46:d1fcf3e4f3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a6dac5989c"></a>
- <a id="s-6e42cd6880"></a>`distribution`: `stove0-target-protocol`
- <a id="s-a2d2fc37de"></a>`module`: `stove0_target_protocol`
- <a id="s-de6913916c"></a>`name`: `canonical_operations`
- <a id="s-4cfbf0c658"></a>`owner`: `stove0_target_protocol.TargetDescriptor`
- <a id="s-b454e895f8"></a>`unit`: `member`

### Declared structure

- <a id="s-fb7b2bc308"></a>`kind`: `"classmethod"`
- <a id="s-96579e5c08"></a>`signature`: `"\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""`

## Maintained corroboration

### Related interface records

- [TargetDescriptor](stove0-target-protocol-targetdescriptor.md)

## Governing policies

- <a id="pa-0c6d8ca307"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetDescriptor.canonical_operations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d5a5e7341e57236ffd42ec65802f1b10652f664a1430a6401ef6e3a8c247246d -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_operations",
  "owner": "stove0_target_protocol.TargetDescriptor",
  "unit": "member"
}
```

</details>
