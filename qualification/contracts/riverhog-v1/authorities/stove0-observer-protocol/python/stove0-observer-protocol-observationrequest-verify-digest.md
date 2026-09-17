# stove0_observer_protocol.ObservationRequest.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationreque-926764b26d:41c39c9ad6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-df0d8174a9"></a>
- <a id="s-e297a964c0"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-7c7da913bb"></a>`module`: `stove0_observer_protocol`
- <a id="s-c8fce8d309"></a>`name`: `verify_digest`
- <a id="s-127a8db76c"></a>`owner`: `stove0_observer_protocol.ObservationRequest`
- <a id="s-9f2829663f"></a>`unit`: `member`

### Declared structure

- <a id="s-2da0af0abc"></a>`kind`: `"method"`
- <a id="s-e6a006d4b3"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ObservationRequest](stove0-observer-protocol-observationrequest.md)

## Governing policies

- <a id="pa-9a0f497da9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationRequest.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 860bfa2e3c8ab17f8bcce2645408fa5c978d179a69e56ce195ee23e3ffb7128c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "verify_digest",
  "owner": "stove0_observer_protocol.ObservationRequest",
  "unit": "member"
}
```

</details>
