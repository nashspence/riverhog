# stove0_protocol.ExecutionEnvelope.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-executionenvelope-seal:294244103e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fb682541b9"></a>
- <a id="s-10d1934037"></a>`distribution`: `stove0-protocol`
- <a id="s-a8cf1add2d"></a>`module`: `stove0_protocol`
- <a id="s-940f1cc912"></a>`name`: `seal`
- <a id="s-e71ce57199"></a>`owner`: `stove0_protocol.ExecutionEnvelope`
- <a id="s-df0e0eb548"></a>`unit`: `member`

### Declared structure

- <a id="s-12ee2ffb19"></a>`kind`: `"classmethod"`
- <a id="s-aa36adf161"></a>`signature`: `"\"(cls, payload: 'ExecutionEnvelopePayload') -> 'ExecutionEnvelope'\""`

## Maintained corroboration

### Related interface records

- [ExecutionEnvelope](stove0-protocol-executionenvelope.md)

## Governing policies

- <a id="pa-057134c0c0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.ExecutionEnvelope.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 788fd253f631cac4d6deda3c488cd551e1bc96c450239b0a3af198ed97d88428 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'ExecutionEnvelopePayload') -> 'ExecutionEnvelope'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.ExecutionEnvelope",
  "unit": "member"
}
```

</details>
