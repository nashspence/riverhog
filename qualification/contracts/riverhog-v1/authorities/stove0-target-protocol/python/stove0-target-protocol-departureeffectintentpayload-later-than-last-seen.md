# stove0_target_protocol.DepartureEffectIntentPayload.later_than_last_seen

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffectint-0b3cec71f0:5b3f7ed1e9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5e8efc0610"></a>
- <a id="s-2871a9bd66"></a>`distribution`: `stove0-target-protocol`
- <a id="s-3f2d185213"></a>`module`: `stove0_target_protocol`
- <a id="s-7a679560a5"></a>`name`: `later_than_last_seen`
- <a id="s-b0c28398a7"></a>`owner`: `stove0_target_protocol.DepartureEffectIntentPayload`
- <a id="s-2599fcdb09"></a>`unit`: `member`

### Declared structure

- <a id="s-a3178bc475"></a>`kind`: `"method"`
- <a id="s-8fe7eacd9d"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectIntentPayload](stove0-target-protocol-departureeffectintentpayload.md)

## Governing policies

- <a id="pa-85dbeff8a9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectIntentPayload.later_than_last_seen`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d338510bb5d2983e570e11f3e6ed14091e232bf6f986fda5190ec286ff44c895 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "later_than_last_seen",
  "owner": "stove0_target_protocol.DepartureEffectIntentPayload",
  "unit": "member"
}
```

</details>
