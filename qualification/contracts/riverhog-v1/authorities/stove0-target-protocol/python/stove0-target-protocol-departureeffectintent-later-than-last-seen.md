# stove0_target_protocol.DepartureEffectIntent.later_than_last_seen

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffectint-2d0776c19e:8b48eabbad -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a8bcff1706"></a>
- <a id="s-6ca328ca0c"></a>`distribution`: `stove0-target-protocol`
- <a id="s-bf03e67af0"></a>`module`: `stove0_target_protocol`
- <a id="s-2f268a0e28"></a>`name`: `later_than_last_seen`
- <a id="s-feb68d4b66"></a>`owner`: `stove0_target_protocol.DepartureEffectIntent`
- <a id="s-189ffd5da4"></a>`unit`: `member`

### Declared structure

- <a id="s-47651025c3"></a>`kind`: `"method"`
- <a id="s-69a901d927"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectIntent](stove0-target-protocol-departureeffectintent.md)

## Governing policies

- <a id="pa-9a781affec"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectIntent.later_than_last_seen`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a91e68dd6fd0c0c0e811bdcc2e126240137735227877f9c3a20ff185bc257c9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "later_than_last_seen",
  "owner": "stove0_target_protocol.DepartureEffectIntent",
  "unit": "member"
}
```

</details>
