# stove0_observer_support.ContentObservationResultBuilder.canceled

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-fea70352f2:e4879269cd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cc6a5d2ee9"></a>
- <a id="s-a75781aa51"></a>`distribution`: `stove0-observer-support`
- <a id="s-e5de2dcd7f"></a>`module`: `stove0_observer_support`
- <a id="s-556e8ea764"></a>`name`: `canceled`
- <a id="s-4467e44379"></a>`owner`: `stove0_observer_support.ContentObservationResultBuilder`
- <a id="s-379e0d29d4"></a>`unit`: `member`

### Declared structure

- <a id="s-63801a3438"></a>`kind`: `"method"`
- <a id="s-18871e7a89"></a>`signature`: `"\"(self, *, execution_evidence: 'Mapping[str, JsonValue] \| None' = None) -> 'ContentObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationResultBuilder](stove0-observer-support-contentobservationresultbuilder.md)

## Governing policies

- <a id="pa-7e99dc255c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationResultBuilder.canceled`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c8ed34b150f423ad31a6fabb1bc1ec7ca07a5894e1d97c44c7efe51931459b4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, execution_evidence: 'Mapping[str, JsonValue] | None' = None) -> 'ContentObservationResult'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "canceled",
  "owner": "stove0_observer_support.ContentObservationResultBuilder",
  "unit": "member"
}
```

</details>
