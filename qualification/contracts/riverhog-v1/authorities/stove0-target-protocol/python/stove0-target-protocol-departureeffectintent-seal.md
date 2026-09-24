# stove0_target_protocol.DepartureEffectIntent.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffectintent-seal:731173f996 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fded2a5243"></a>
- <a id="s-02a4d015e7"></a>`distribution`: `stove0-target-protocol`
- <a id="s-11c9f03aac"></a>`module`: `stove0_target_protocol`
- <a id="s-424ab2a7b2"></a>`name`: `seal`
- <a id="s-1b1736eb6f"></a>`owner`: `stove0_target_protocol.DepartureEffectIntent`
- <a id="s-868e4f725f"></a>`unit`: `member`

### Declared structure

- <a id="s-fef0b25682"></a>`kind`: `"classmethod"`
- <a id="s-e710709b56"></a>`signature`: `"\"(cls, payload: 'DepartureEffectIntentPayload') -> 'DepartureEffectIntent'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectIntent](stove0-target-protocol-departureeffectintent.md)

## Governing policies

- <a id="pa-3c32a30548"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectIntent.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5d2b2cc4016563ae7089746d83e5922bfc0d72922f6e98297a19bc383b8d1921 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'DepartureEffectIntentPayload') -> 'DepartureEffectIntent'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal",
  "owner": "stove0_target_protocol.DepartureEffectIntent",
  "unit": "member"
}
```

</details>
