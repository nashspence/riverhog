# stove0_observer_protocol.ContentObservationRequest.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-contentobservati-3013f024f0:e8f5106df3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3a4c8860a7"></a>
- <a id="s-d80cc937f9"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-174a02c7ea"></a>`module`: `stove0_observer_protocol`
- <a id="s-569826827a"></a>`name`: `verify_digest`
- <a id="s-59180c69c4"></a>`owner`: `stove0_observer_protocol.ContentObservationRequest`
- <a id="s-a13e2b0e05"></a>`unit`: `member`

### Declared structure

- <a id="s-6a82897beb"></a>`kind`: `"method"`
- <a id="s-b7613dae43"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationRequest](stove0-observer-protocol-contentobservationrequest.md)

## Governing policies

- <a id="pa-38a63e3a2f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ContentObservationRequest.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d8970a171ef4fcea78bb39b90266700246f7f6d708eaf2be6a166543069b526f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "verify_digest",
  "owner": "stove0_observer_protocol.ContentObservationRequest",
  "unit": "member"
}
```

</details>
