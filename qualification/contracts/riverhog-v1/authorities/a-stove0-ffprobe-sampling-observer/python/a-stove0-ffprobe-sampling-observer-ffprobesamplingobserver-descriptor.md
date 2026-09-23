# a_stove0_ffprobe_sampling_observer.FfprobeSamplingObserver.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-ffprobe-sampling-observer:a-stove0-ffprobe-sampling-observer-ffprob-9233da9347:d96e14293c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a75c1f803d"></a>
- <a id="s-620cc68145"></a>`distribution`: `a-stove0-ffprobe-sampling-observer`
- <a id="s-8d9319851f"></a>`module`: `a_stove0_ffprobe_sampling_observer`
- <a id="s-1347d0f2f8"></a>`name`: `descriptor`
- <a id="s-742e46f48e"></a>`owner`: `a_stove0_ffprobe_sampling_observer.FfprobeSamplingObserver`
- <a id="s-46ef6f1689"></a>`unit`: `member`

### Declared structure

- <a id="s-8348741e41"></a>`kind`: `"method"`
- <a id="s-d11763d039"></a>`signature`: `"\"(self) -> 'ObserverDescriptor'\""`

## Maintained corroboration

### Related interface records

- [FfprobeSamplingObserver](a-stove0-ffprobe-sampling-observer-ffprobesamplingobserver.md)

## Governing policies

- <a id="pa-d3f1843559"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-ffprobe-sampling-observer:a_stove0_ffprobe_sampling_observer](../../../evidence/sources/authorities.md#src-bbde6c899e) — [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_ffprobe_sampling_observer.FfprobeSamplingObserver.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b9ee6649b0df9103dd2c466ce9adf2b922b55f76fef516dea98f12ca8366cac2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'ObserverDescriptor'\""
  },
  "distribution": "a-stove0-ffprobe-sampling-observer",
  "module": "a_stove0_ffprobe_sampling_observer",
  "name": "descriptor",
  "owner": "a_stove0_ffprobe_sampling_observer.FfprobeSamplingObserver",
  "unit": "member"
}
```

</details>
