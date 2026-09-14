# stove0_media_sampling_observer_contracts.SampleableRange

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-sampling-observer-contracts:stove0-media-sampling-observer-contracts-60d1a1baab:479d698010 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-sampling-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9c87acab97"></a>
- <a id="s-ca6c5bba05"></a>`distribution`: `stove0-media-sampling-observer-contracts`
- <a id="s-cab3e5577d"></a>`module`: `stove0_media_sampling_observer_contracts`
- <a id="s-8b9011fb4d"></a>`name`: `SampleableRange`
- <a id="s-08637c79bd"></a>`unit`: `export`

### Declared structure

- <a id="s-6c17b3396d"></a>`kind`: `"class"`
- <a id="s-d2ca5c2857"></a>`signature`: `"'(*, start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)]) -> None'"`

#### Validated model schema

<a id="s-1db9d030a4"></a>
- <a id="s-a0b240086f"></a>`title`: SampleableRange
- <a id="s-78c3c2776f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7399363eaa"></a>`duration_ms` | yes | type="integer"; minimum=1 |  |
| <a id="s-8c0f3519ce"></a>`start_ms` | yes | type="integer"; minimum=0 |  |

## Governing policies

- <a id="pa-c0bd866587"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-sampling-observer-contracts:stove0_media_sampling_observer_contracts](../../../evidence/sources.md#src-b9344d06d7) — `reference/stove0/observers/contracts/media-sampling/src/stove0_media_sampling_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_sampling_observer_contracts.SampleableRange`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d2a74811235f9621ae1a3e948e9d418d9e117e6f90b1082e65835a16eb88200 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "duration_ms": {
          "minimum": 1,
          "title": "Duration Ms",
          "type": "integer"
        },
        "start_ms": {
          "minimum": 0,
          "title": "Start Ms",
          "type": "integer"
        }
      },
      "required": [
        "start_ms",
        "duration_ms"
      ],
      "title": "SampleableRange",
      "type": "object"
    },
    "signature": "'(*, start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "stove0-media-sampling-observer-contracts",
  "module": "stove0_media_sampling_observer_contracts",
  "name": "SampleableRange",
  "unit": "export"
}
```
