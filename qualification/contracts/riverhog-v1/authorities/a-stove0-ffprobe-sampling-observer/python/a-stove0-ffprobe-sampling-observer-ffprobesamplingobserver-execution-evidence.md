# a_stove0_ffprobe_sampling_observer.FfprobeSamplingObserver.execution_evidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-ffprobe-sampling-observer:a-stove0-ffprobe-sampling-observer-ffprob-2e70a66a81:9d6fa6215a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c051ec7592"></a>
- <a id="s-7b514f0b7c"></a>`distribution`: `a-stove0-ffprobe-sampling-observer`
- <a id="s-9ace8b5d85"></a>`module`: `a_stove0_ffprobe_sampling_observer`
- <a id="s-c4afa5a0c7"></a>`name`: `execution_evidence`
- <a id="s-cfea2da777"></a>`owner`: `a_stove0_ffprobe_sampling_observer.FfprobeSamplingObserver`
- <a id="s-cd595703f0"></a>`unit`: `member`

### Declared structure

- <a id="s-e0dd509e95"></a>`kind`: `"method"`
- <a id="s-b10f72fbfb"></a>`signature`: `"\"(self) -> 'dict[str, str]'\""`

## Maintained corroboration

### Related interface records

- [FfprobeSamplingObserver](a-stove0-ffprobe-sampling-observer-ffprobesamplingobserver.md)

## Governing policies

- <a id="pa-39c766f9c0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-ffprobe-sampling-observer:a_stove0_ffprobe_sampling_observer](../../../evidence/sources/authorities.md#src-bbde6c899e) — [some-implementations/stove0/observers/ffprobe-sampling/src/a\_stove0\_ffprobe\_sampling\_observer/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/ffprobe-sampling/src/a_stove0_ffprobe_sampling_observer/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_ffprobe_sampling_observer.FfprobeSamplingObserver.execution_evidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7498a33a1cfe065e88cae13f18c5c8d185ee0740e1a159ac0b9c93724baa1afe -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, str]'\""
  },
  "distribution": "a-stove0-ffprobe-sampling-observer",
  "module": "a_stove0_ffprobe_sampling_observer",
  "name": "execution_evidence",
  "owner": "a_stove0_ffprobe_sampling_observer.FfprobeSamplingObserver",
  "unit": "member"
}
```

</details>
