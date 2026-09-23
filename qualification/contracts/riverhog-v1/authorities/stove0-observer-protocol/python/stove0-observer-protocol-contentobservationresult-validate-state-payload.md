# stove0_observer_protocol.ContentObservationResult.validate_state_payload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-contentobservati-7d5f9374aa:a7680ee397 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-163708a246"></a>
- <a id="s-2b51a95263"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-d6b072f984"></a>`module`: `stove0_observer_protocol`
- <a id="s-d539687402"></a>`name`: `validate_state_payload`
- <a id="s-10de988d05"></a>`owner`: `stove0_observer_protocol.ContentObservationResult`
- <a id="s-772310fd8c"></a>`unit`: `member`

### Declared structure

- <a id="s-d1a50f6e19"></a>`kind`: `"method"`
- <a id="s-f429d8b0dd"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationResult](stove0-observer-protocol-contentobservationresult.md)

## Governing policies

- <a id="pa-5c0b164a26"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ContentObservationResult.validate_state_payload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 01d50f9c4cdc47681c7de8b6a9d8599b93c4ca0374377de1ed24ccde450350d1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "validate_state_payload",
  "owner": "stove0_observer_protocol.ContentObservationResult",
  "unit": "member"
}
```

</details>
