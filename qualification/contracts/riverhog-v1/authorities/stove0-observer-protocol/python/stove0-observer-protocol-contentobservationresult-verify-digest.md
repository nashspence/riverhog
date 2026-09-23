# stove0_observer_protocol.ContentObservationResult.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-contentobservati-77465f1dc0:5970c59cd4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ee5ed0fe9d"></a>
- <a id="s-a570956213"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-35c97c0144"></a>`module`: `stove0_observer_protocol`
- <a id="s-7181c4963d"></a>`name`: `verify_digest`
- <a id="s-8087ca0db7"></a>`owner`: `stove0_observer_protocol.ContentObservationResult`
- <a id="s-c7c5ef517c"></a>`unit`: `member`

### Declared structure

- <a id="s-67fb189c26"></a>`kind`: `"method"`
- <a id="s-0826a35a11"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationResult](stove0-observer-protocol-contentobservationresult.md)

## Governing policies

- <a id="pa-c421fa923c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ContentObservationResult.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 50b3d01ac4672f8985a5368849a4286718d3b696c7291ff9c06adcf2eb7c7b15 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "verify_digest",
  "owner": "stove0_observer_protocol.ContentObservationResult",
  "unit": "member"
}
```

</details>
