# a_stove0_ffprobe_sampling_observer.FfprobeSamplingObserver

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-ffprobe-sampling-observer:a-stove0-ffprobe-sampling-observer-ffprob-ceb0b27581:9c73780664 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3ab4941ade"></a>
- <a id="s-233e23a51c"></a>`distribution`: `a-stove0-ffprobe-sampling-observer`
- <a id="s-9c05af58e9"></a>`module`: `a_stove0_ffprobe_sampling_observer`
- <a id="s-2e58f01ceb"></a>`name`: `FfprobeSamplingObserver`
- <a id="s-a637ddbbd9"></a>`unit`: `export`

### Declared structure

- <a id="s-80b6651bba"></a>`kind`: `"class"`
- <a id="s-e8ee3e81ce"></a>`signature`: `"\"(*, ffprobe: 'str' = 'ffprobe', workspace_root: 'Path \| None' = None, source_revision: 'str' = 'unknown', image_id: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [observe](a-stove0-ffprobe-sampling-observer-ffprobesamplingobserver-observe.md)
- [execution_evidence](a-stove0-ffprobe-sampling-observer-ffprobesamplingobserver-execution-evidence.md)
- [descriptor](a-stove0-ffprobe-sampling-observer-ffprobesamplingobserver-descriptor.md)

## Governing policies

- <a id="pa-a7f6167a57"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-ffprobe-sampling-observer:a_stove0_ffprobe_sampling_observer](../../../evidence/sources/authorities.md#src-bbde6c899e) — [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_ffprobe_sampling_observer.FfprobeSamplingObserver`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b8b92c055e315f85051dfba2d9f36ebeccab9400f46c988e79b0a958e434854 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, ffprobe: 'str' = 'ffprobe', workspace_root: 'Path | None' = None, source_revision: 'str' = 'unknown', image_id: 'str') -> 'None'\""
  },
  "distribution": "a-stove0-ffprobe-sampling-observer",
  "module": "a_stove0_ffprobe_sampling_observer",
  "name": "FfprobeSamplingObserver",
  "unit": "export"
}
```

</details>
