# stove0_observer_protocol.ContentObservationRequest.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-contentobservati-073eb1ace7:5b8eceaf9d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf5365039b"></a>
- <a id="s-46e8be870b"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-940f0e745f"></a>`module`: `stove0_observer_protocol`
- <a id="s-0f29147642"></a>`name`: `seal`
- <a id="s-1e6c28e6aa"></a>`owner`: `stove0_observer_protocol.ContentObservationRequest`
- <a id="s-c7306ae5db"></a>`unit`: `member`

### Declared structure

- <a id="s-af967dc73d"></a>`kind`: `"classmethod"`
- <a id="s-ed28cea8a4"></a>`signature`: `"\"(cls, payload: 'ContentObservationRequestPayload') -> 'ContentObservationRequest'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationRequest](stove0-observer-protocol-contentobservationrequest.md)

## Governing policies

- <a id="pa-eebf80b1f2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ContentObservationRequest.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9715b29e1a435e124b1c64b45762fc499f6f867f751bc1cfde1b15cda4b27080 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'ContentObservationRequestPayload') -> 'ContentObservationRequest'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "seal",
  "owner": "stove0_observer_protocol.ContentObservationRequest",
  "unit": "member"
}
```

</details>
