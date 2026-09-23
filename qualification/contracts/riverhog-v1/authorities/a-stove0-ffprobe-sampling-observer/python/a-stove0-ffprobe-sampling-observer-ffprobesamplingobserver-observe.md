# a_stove0_ffprobe_sampling_observer.FfprobeSamplingObserver.observe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-ffprobe-sampling-observer:a-stove0-ffprobe-sampling-observer-ffprob-051ecef979:ffce77dd85 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f662fd597e"></a>
- <a id="s-6e9cacc535"></a>`distribution`: `a-stove0-ffprobe-sampling-observer`
- <a id="s-9fb849f104"></a>`module`: `a_stove0_ffprobe_sampling_observer`
- <a id="s-f93baf1545"></a>`name`: `observe`
- <a id="s-df213bdcf1"></a>`owner`: `a_stove0_ffprobe_sampling_observer.FfprobeSamplingObserver`
- <a id="s-9e5a896c43"></a>`unit`: `member`

### Declared structure

- <a id="s-d6180e7b3c"></a>`kind`: `"method"`
- <a id="s-19b2ef1149"></a>`signature`: `"\"(self, request: 'ContentObservationRequest', runtime: 'ContentObservationRuntime') -> 'ContentObservationResult'\""`

## Maintained corroboration

### Related interface records

- [FfprobeSamplingObserver](a-stove0-ffprobe-sampling-observer-ffprobesamplingobserver.md)

## Governing policies

- <a id="pa-b7cb4bac3e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-ffprobe-sampling-observer:a_stove0_ffprobe_sampling_observer](../../../evidence/sources/authorities.md#src-bbde6c899e) — [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_ffprobe_sampling_observer.FfprobeSamplingObserver.observe`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0274492baadb362f78b579ac672dc634e3c9939212705728e99b82e6dd8adf4b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ContentObservationRequest', runtime: 'ContentObservationRuntime') -> 'ContentObservationResult'\""
  },
  "distribution": "a-stove0-ffprobe-sampling-observer",
  "module": "a_stove0_ffprobe_sampling_observer",
  "name": "observe",
  "owner": "a_stove0_ffprobe_sampling_observer.FfprobeSamplingObserver",
  "unit": "member"
}
```

</details>
