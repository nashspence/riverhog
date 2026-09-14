# stove0_ffprobe_sampling_observer.FfprobeSamplingObserver.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-ffprobes-c6f6dc1727:6ae8a2606e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bd9b9f612c"></a>
- <a id="s-e51f73ff44"></a>`distribution`: `stove0-ffprobe-sampling-observer`
- <a id="s-c866971436"></a>`module`: `stove0_ffprobe_sampling_observer`
- <a id="s-36c550f91b"></a>`name`: `descriptor`
- <a id="s-feb41e928c"></a>`owner`: `stove0_ffprobe_sampling_observer.FfprobeSamplingObserver`
- <a id="s-b6eb41ab5a"></a>`unit`: `member`

### Declared structure

- <a id="s-d02816b8bf"></a>`kind`: `"method"`
- <a id="s-f09c4bad41"></a>`signature`: `"\"(self) -> 'ObserverDescriptor'\""`

## Maintained corroboration

### Related interface records

- [stove0_ffprobe_sampling_observer.FfprobeSamplingObserver](stove0-ffprobe-sampling-observer-ffprobesamplingobserver.md)

## Governing policies

- <a id="pa-7a88694ad1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-ffprobe-sampling-observer:stove0_ffprobe_sampling_observer](../../../evidence/sources.md#src-7974c3bc25) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/__init__.py`

### Machine authority

- `/external_contract/python/stove0_ffprobe_sampling_observer.FfprobeSamplingObserver.descriptor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 58f584c03dd37b189df0f1b0c8a70511c719f79cc3082f3519cf53f7c35b5e48 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'ObserverDescriptor'\""
  },
  "distribution": "stove0-ffprobe-sampling-observer",
  "module": "stove0_ffprobe_sampling_observer",
  "name": "descriptor",
  "owner": "stove0_ffprobe_sampling_observer.FfprobeSamplingObserver",
  "unit": "member"
}
```
