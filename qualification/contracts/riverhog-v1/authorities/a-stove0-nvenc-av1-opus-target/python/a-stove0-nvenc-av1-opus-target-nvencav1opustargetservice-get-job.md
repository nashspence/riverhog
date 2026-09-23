# a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.get_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-nvenc-av1-opus-target:a-stove0-nvenc-av1-opus-target-nvencav1op-5427edd5b7:5c0b83fc72 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-97aca58e05"></a>
- <a id="s-a9a3acce3b"></a>`distribution`: `a-stove0-nvenc-av1-opus-target`
- <a id="s-4328b652c7"></a>`module`: `a_stove0_nvenc_av1_opus_target`
- <a id="s-65753f7849"></a>`name`: `get_job`
- <a id="s-ebf03a9cfc"></a>`owner`: `a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService`
- <a id="s-8cf04d3fd6"></a>`unit`: `member`

### Declared structure

- <a id="s-ba082761dd"></a>`kind`: `"method"`
- <a id="s-b9df2ce353"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [NvencAv1OpusTargetService](a-stove0-nvenc-av1-opus-target-nvencav1opustargetservice.md)

## Governing policies

- <a id="pa-875a4f753c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-nvenc-av1-opus-target:a_stove0_nvenc_av1_opus_target](../../../evidence/sources/authorities.md#src-a28a72aec7) — [some-implementations/stove0/targets/nvenc-av1-opus/target/src/a\_stove0\_nvenc\_av1\_opus\_target/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.get_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e5db0056d71825a55c4ab9dd4f6ac5d97313406dfec410af16ead7130c9bdf8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "a-stove0-nvenc-av1-opus-target",
  "module": "a_stove0_nvenc_av1_opus_target",
  "name": "get_job",
  "owner": "a_stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService",
  "unit": "member"
}
```

</details>
