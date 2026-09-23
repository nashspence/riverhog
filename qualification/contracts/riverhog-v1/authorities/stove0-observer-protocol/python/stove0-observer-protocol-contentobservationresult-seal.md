# stove0_observer_protocol.ContentObservationResult.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-contentobservati-0c5e6b757f:48d9eae57a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-613fe36eca"></a>
- <a id="s-439d53e129"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-f4531dd615"></a>`module`: `stove0_observer_protocol`
- <a id="s-67cf07a715"></a>`name`: `seal`
- <a id="s-39950cfd92"></a>`owner`: `stove0_observer_protocol.ContentObservationResult`
- <a id="s-a716d18d76"></a>`unit`: `member`

### Declared structure

- <a id="s-5c95a191dd"></a>`kind`: `"classmethod"`
- <a id="s-56ada4ba28"></a>`signature`: `"\"(cls, payload: 'ContentObservationResultPayload') -> 'ContentObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationResult](stove0-observer-protocol-contentobservationresult.md)

## Governing policies

- <a id="pa-c3b2ce2399"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ContentObservationResult.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a83b7e2779d7ebb7c0e8e0db67740bb64c04edf9b8787a9e0f26610ac949c38 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'ContentObservationResultPayload') -> 'ContentObservationResult'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "seal",
  "owner": "stove0_observer_protocol.ContentObservationResult",
  "unit": "member"
}
```

</details>
