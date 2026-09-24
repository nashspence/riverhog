# stove0_target_protocol.DepartureEffectTargetDescriptor.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffecttar-5e3c145313:e263e5429e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ea99aba64d"></a>
- <a id="s-37235f44b2"></a>`distribution`: `stove0-target-protocol`
- <a id="s-bb0081bf44"></a>`module`: `stove0_target_protocol`
- <a id="s-bdb8e064be"></a>`name`: `seal`
- <a id="s-a822344460"></a>`owner`: `stove0_target_protocol.DepartureEffectTargetDescriptor`
- <a id="s-9e68d01032"></a>`unit`: `member`

### Declared structure

- <a id="s-9bfc9ff073"></a>`kind`: `"classmethod"`
- <a id="s-e77a3b26b6"></a>`signature`: `"\"(cls, payload: 'DepartureEffectTargetDescriptorPayload') -> 'DepartureEffectTargetDescriptor'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectTargetDescriptor](stove0-target-protocol-departureeffecttargetdescriptor.md)

## Governing policies

- <a id="pa-a863de61d6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectTargetDescriptor.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8a1d237cbe5f6c38e4e7985d919c1bd49cc55ac166528774b5abef4ce8327ba3 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'DepartureEffectTargetDescriptorPayload') -> 'DepartureEffectTargetDescriptor'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal",
  "owner": "stove0_target_protocol.DepartureEffectTargetDescriptor",
  "unit": "member"
}
```

</details>
