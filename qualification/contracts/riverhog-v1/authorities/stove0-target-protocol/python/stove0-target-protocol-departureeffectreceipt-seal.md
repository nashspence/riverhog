# stove0_target_protocol.DepartureEffectReceipt.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffectreceipt-seal:596d17d0bd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-23b1e7ca10"></a>
- <a id="s-17af0fffca"></a>`distribution`: `stove0-target-protocol`
- <a id="s-99c796b670"></a>`module`: `stove0_target_protocol`
- <a id="s-313acf197c"></a>`name`: `seal`
- <a id="s-886380b661"></a>`owner`: `stove0_target_protocol.DepartureEffectReceipt`
- <a id="s-d9df3b91df"></a>`unit`: `member`

### Declared structure

- <a id="s-ea19e04f7c"></a>`kind`: `"classmethod"`
- <a id="s-d5ec4a71ec"></a>`signature`: `"\"(cls, payload: 'DepartureEffectReceiptPayload') -> 'DepartureEffectReceipt'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectReceipt](stove0-target-protocol-departureeffectreceipt.md)

## Governing policies

- <a id="pa-32ae510c5b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectReceipt.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 36d10075aa1c15f3d110a4ac01bd8ba0bd19de499e37f278e6d4976a1770dff5 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'DepartureEffectReceiptPayload') -> 'DepartureEffectReceipt'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal",
  "owner": "stove0_target_protocol.DepartureEffectReceipt",
  "unit": "member"
}
```

</details>
