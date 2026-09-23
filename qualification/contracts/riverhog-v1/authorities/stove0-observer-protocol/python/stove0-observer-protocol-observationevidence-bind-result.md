# stove0_observer_protocol.ObservationEvidence.bind_result

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationevide-1aaa7b50eb:c38e8a369b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9ecc8e68f7"></a>
- <a id="s-60ca32cf18"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-2034c1fd9e"></a>`module`: `stove0_observer_protocol`
- <a id="s-3a4759e4b8"></a>`name`: `bind_result`
- <a id="s-22b639278a"></a>`owner`: `stove0_observer_protocol.ObservationEvidence`
- <a id="s-9f1a69fa3f"></a>`unit`: `member`

### Declared structure

- <a id="s-5f14679199"></a>`kind`: `"method"`
- <a id="s-eda03e8adf"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ObservationEvidence](stove0-observer-protocol-observationevidence.md)

## Governing policies

- <a id="pa-0946b8dede"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationEvidence.bind_result`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 366f7a8547adb232a31f3ec83641cdca571e6371c3847358ac1fef610c6a9b6c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "bind_result",
  "owner": "stove0_observer_protocol.ObservationEvidence",
  "unit": "member"
}
```

</details>
