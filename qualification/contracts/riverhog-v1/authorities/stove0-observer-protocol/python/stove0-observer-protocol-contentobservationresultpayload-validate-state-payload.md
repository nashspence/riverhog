# stove0_observer_protocol.ContentObservationResultPayload.validate_state_payload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-contentobservati-09b5d3189b:2cfded334b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4924908d96"></a>
- <a id="s-e17fd81d3c"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-63c17a6357"></a>`module`: `stove0_observer_protocol`
- <a id="s-e203fd63b7"></a>`name`: `validate_state_payload`
- <a id="s-0c5aa46c34"></a>`owner`: `stove0_observer_protocol.ContentObservationResultPayload`
- <a id="s-42e0b9840a"></a>`unit`: `member`

### Declared structure

- <a id="s-925ce5ea29"></a>`kind`: `"method"`
- <a id="s-83059f01ec"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationResultPayload](stove0-observer-protocol-contentobservationresultpayload.md)

## Governing policies

- <a id="pa-57e275ae16"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ContentObservationResultPayload.validate_state_payload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 902b0844f607936fac2c504e55e1f23c17d709caf9cbe8fc17de2a023a52a4d0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "validate_state_payload",
  "owner": "stove0_observer_protocol.ContentObservationResultPayload",
  "unit": "member"
}
```

</details>
