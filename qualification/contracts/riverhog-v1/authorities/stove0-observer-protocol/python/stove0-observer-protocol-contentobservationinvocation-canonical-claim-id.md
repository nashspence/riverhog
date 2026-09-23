# stove0_observer_protocol.ContentObservationInvocation.canonical_claim_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-contentobservati-ab8bdb0b92:613ef05b81 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5cdea52540"></a>
- <a id="s-53e836d4fa"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-bdfb465e67"></a>`module`: `stove0_observer_protocol`
- <a id="s-1de2b6094c"></a>`name`: `canonical_claim_id`
- <a id="s-eabc933d9e"></a>`owner`: `stove0_observer_protocol.ContentObservationInvocation`
- <a id="s-7d7fa373e1"></a>`unit`: `member`

### Declared structure

- <a id="s-ac7690daa3"></a>`kind`: `"classmethod"`
- <a id="s-ff9c2f3f01"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationInvocation](stove0-observer-protocol-contentobservationinvocation.md)

## Governing policies

- <a id="pa-438d1b5b12"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ContentObservationInvocation.canonical_claim_id`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 51c4876313fddc24cb576d032ddd2638bd63d212d82d1113c5dda358447db6d6 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_claim_id",
  "owner": "stove0_observer_protocol.ContentObservationInvocation",
  "unit": "member"
}
```

</details>
