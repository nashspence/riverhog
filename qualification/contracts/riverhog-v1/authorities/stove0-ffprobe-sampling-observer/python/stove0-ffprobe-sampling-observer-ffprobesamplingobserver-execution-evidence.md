# stove0_ffprobe_sampling_observer.FfprobeSamplingObserver.execution_evidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-ffprobes-5a3e04b8be:e2e22c2f59 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4da5a6fce8"></a>
- <a id="s-be55685aa4"></a>`distribution`: `stove0-ffprobe-sampling-observer`
- <a id="s-c6cc35d16b"></a>`module`: `stove0_ffprobe_sampling_observer`
- <a id="s-9eea8ffdf9"></a>`name`: `execution_evidence`
- <a id="s-3d7d60c5ff"></a>`owner`: `stove0_ffprobe_sampling_observer.FfprobeSamplingObserver`
- <a id="s-e71433da5f"></a>`unit`: `member`

### Declared structure

- <a id="s-bff8b6459f"></a>`kind`: `"method"`
- <a id="s-8c5122137c"></a>`signature`: `"\"(self) -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [FfprobeSamplingObserver](stove0-ffprobe-sampling-observer-ffprobesamplingobserver.md)

## Governing policies

- <a id="pa-0e210fb558"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-ffprobe-sampling-observer:stove0_ffprobe_sampling_observer](../../../evidence/sources.md#src-7974c3bc25) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/__init__.py`

### Machine authority

- `/external_contract/python/stove0_ffprobe_sampling_observer.FfprobeSamplingObserver.execution_evidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f5a238f0c896ddc8183c4878cb7595d69746c6722a24a1d112116c4facf2a81 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, str]'\""
  },
  "distribution": "stove0-ffprobe-sampling-observer",
  "module": "stove0_ffprobe_sampling_observer",
  "name": "execution_evidence",
  "owner": "stove0_ffprobe_sampling_observer.FfprobeSamplingObserver",
  "unit": "member"
}
```

</details>
