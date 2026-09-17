# stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.put_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-nvencav1opus-98c98e85c5:45c786e2a2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c84f7c8226"></a>
- <a id="s-fd6ba22b60"></a>`distribution`: `stove0-nvenc-av1-opus-target`
- <a id="s-7a6fde0065"></a>`module`: `stove0_nvenc_av1_opus_target`
- <a id="s-215bae8cd5"></a>`name`: `put_job`
- <a id="s-1533d96e1b"></a>`owner`: `stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService`
- <a id="s-d792e59e63"></a>`unit`: `member`

### Declared structure

- <a id="s-4d36ea6bb6"></a>`kind`: `"method"`
- <a id="s-4e897eadda"></a>`signature`: `"\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [NvencAv1OpusTargetService](stove0-nvenc-av1-opus-target-nvencav1opustargetservice.md)

## Governing policies

- <a id="pa-f91a9c1cc5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-nvenc-av1-opus-target:stove0_nvenc_av1_opus_target](../../../evidence/sources/authorities.md#src-b6e7b93ef1) — [reference/stove0/targets/nvenc-av1-opus/target/src/stove0\_nvenc\_av1\_opus\_target/\_\_init\_\_.py](../../../../../../reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.put_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8d3643efbb4115c053cf5d50fec8ebd3d9f9d48ce90e6d561a41911c368b1e8b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-nvenc-av1-opus-target",
  "module": "stove0_nvenc_av1_opus_target",
  "name": "put_job",
  "owner": "stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService",
  "unit": "member"
}
```

</details>
