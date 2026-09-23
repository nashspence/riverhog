# stove0_target_protocol.TargetDescriptor.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetdescriptor-seal:c5deabe154 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f9ee5f27ab"></a>
- <a id="s-515d73def1"></a>`distribution`: `stove0-target-protocol`
- <a id="s-c156be972e"></a>`module`: `stove0_target_protocol`
- <a id="s-99775e9789"></a>`name`: `seal`
- <a id="s-9402b124ea"></a>`owner`: `stove0_target_protocol.TargetDescriptor`
- <a id="s-9783cce8c9"></a>`unit`: `member`

### Declared structure

- <a id="s-70d5c568bb"></a>`kind`: `"classmethod"`
- <a id="s-1332ec914d"></a>`signature`: `"\"(cls, payload: 'TargetDescriptorPayload') -> 'TargetDescriptor'\""`

## Maintained corroboration

### Related interface records

- [TargetDescriptor](stove0-target-protocol-targetdescriptor.md)

## Governing policies

- <a id="pa-417d11005a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetDescriptor.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fec33808d98c0f502836a5ff50d7f6d91f5a1a7e02d46872b60bcb14ab264328 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'TargetDescriptorPayload') -> 'TargetDescriptor'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal",
  "owner": "stove0_target_protocol.TargetDescriptor",
  "unit": "member"
}
```

</details>
