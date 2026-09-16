# stove0_ffprobe_sampling_observer.FfprobeSamplingObserver

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-ffprobes-ce2fd3e51b:c15e61f200 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7955888587"></a>
- <a id="s-3e3ca77399"></a>`distribution`: `stove0-ffprobe-sampling-observer`
- <a id="s-793f2f6ad9"></a>`module`: `stove0_ffprobe_sampling_observer`
- <a id="s-511d8bd0cb"></a>`name`: `FfprobeSamplingObserver`
- <a id="s-ceed2a9b56"></a>`unit`: `export`

### Declared structure

- <a id="s-8e9039f1ae"></a>`kind`: `"class"`
- <a id="s-fd70a821b2"></a>`signature`: `"\"(*, ffprobe: 'str' = 'ffprobe', workspace_root: 'Path \| None' = None, source_revision: 'str' = 'unknown', image_digest: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [observe](stove0-ffprobe-sampling-observer-ffprobesamplingobserver-observe.md)
- [execution_evidence](stove0-ffprobe-sampling-observer-ffprobesamplingobserver-execution-evidence.md)
- [descriptor](stove0-ffprobe-sampling-observer-ffprobesamplingobserver-descriptor.md)

## Governing policies

- <a id="pa-ffab656bee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-ffprobe-sampling-observer:stove0_ffprobe_sampling_observer](../../../evidence/sources.md#src-7974c3bc25) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/__init__.py`

### Machine authority

- `/external_contract/python/stove0_ffprobe_sampling_observer.FfprobeSamplingObserver`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a0761e0807a2457f04638c0f419e55f44749cbc51d6416149283f9f0733f836c -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, ffprobe: 'str' = 'ffprobe', workspace_root: 'Path | None' = None, source_revision: 'str' = 'unknown', image_digest: 'str') -> 'None'\""
  },
  "distribution": "stove0-ffprobe-sampling-observer",
  "module": "stove0_ffprobe_sampling_observer",
  "name": "FfprobeSamplingObserver",
  "unit": "export"
}
```

</details>
