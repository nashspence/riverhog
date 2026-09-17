# stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.cancel_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-nvencav1opus-df9e02d6d8:c60e9bd4d3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-159a5c7d3c"></a>
- <a id="s-5b9df43f9a"></a>`distribution`: `stove0-nvenc-av1-opus-target`
- <a id="s-044e536bae"></a>`module`: `stove0_nvenc_av1_opus_target`
- <a id="s-6672690de5"></a>`name`: `cancel_job`
- <a id="s-fe52ac3b42"></a>`owner`: `stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService`
- <a id="s-a542717d78"></a>`unit`: `member`

### Declared structure

- <a id="s-ced8c1ac6a"></a>`kind`: `"method"`
- <a id="s-920590f95c"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [NvencAv1OpusTargetService](stove0-nvenc-av1-opus-target-nvencav1opustargetservice.md)

## Governing policies

- <a id="pa-6131b22fd2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-nvenc-av1-opus-target:stove0_nvenc_av1_opus_target](../../../evidence/sources.md#src-b6e7b93ef1) — [reference/stove0/targets/nvenc-av1-opus/target/src/stove0\_nvenc\_av1\_opus\_target/\_\_init\_\_.py](../../../../../../reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.cancel_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9a63d17af43bd6264f6b323c24f9c2ed7d34008db6c25c21a5667832abc932e1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-nvenc-av1-opus-target",
  "module": "stove0_nvenc_av1_opus_target",
  "name": "cancel_job",
  "owner": "stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService",
  "unit": "member"
}
```

</details>
