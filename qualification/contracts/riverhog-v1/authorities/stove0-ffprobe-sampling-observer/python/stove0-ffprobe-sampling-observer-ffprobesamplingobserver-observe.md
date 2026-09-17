# stove0_ffprobe_sampling_observer.FfprobeSamplingObserver.observe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-ffprobes-527f6d4c5d:d1f368fa0c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6e2581f441"></a>
- <a id="s-7fd330480c"></a>`distribution`: `stove0-ffprobe-sampling-observer`
- <a id="s-4a959affe9"></a>`module`: `stove0_ffprobe_sampling_observer`
- <a id="s-9df625ec46"></a>`name`: `observe`
- <a id="s-7ca6d250d8"></a>`owner`: `stove0_ffprobe_sampling_observer.FfprobeSamplingObserver`
- <a id="s-cdcc1ff240"></a>`unit`: `member`

### Declared structure

- <a id="s-ce7cc26ee1"></a>`kind`: `"method"`
- <a id="s-454dfb6c37"></a>`signature`: `"\"(self, request: 'ObservationRequest', runtime: 'ObservationRuntime') -> 'ObservationResult'\""`

## Maintained corroboration

### Related interface records

- [FfprobeSamplingObserver](stove0-ffprobe-sampling-observer-ffprobesamplingobserver.md)

## Governing policies

- <a id="pa-c5225e1574"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-ffprobe-sampling-observer:stove0_ffprobe_sampling_observer](../../../evidence/sources.md#src-7974c3bc25) — [reference/stove0/observers/ffprobe-sampling/src/stove0\_ffprobe\_sampling\_observer/\_\_init\_\_.py](../../../../../../reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/__init__.py)

### Machine authority

- `/external_contract/python/stove0_ffprobe_sampling_observer.FfprobeSamplingObserver.observe`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f14be6f62d9b2bfa9ebb7f8ac13a38093d58af5974eb6e3ae9b58b93a1c43989 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ObservationRequest', runtime: 'ObservationRuntime') -> 'ObservationResult'\""
  },
  "distribution": "stove0-ffprobe-sampling-observer",
  "module": "stove0_ffprobe_sampling_observer",
  "name": "observe",
  "owner": "stove0_ffprobe_sampling_observer.FfprobeSamplingObserver",
  "unit": "member"
}
```

</details>
