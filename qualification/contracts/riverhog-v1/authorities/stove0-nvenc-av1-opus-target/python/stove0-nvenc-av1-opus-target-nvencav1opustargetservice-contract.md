# stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-nvencav1opus-d38a0a7e5e:6d0fe51c68 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ff9b072377"></a>
- <a id="s-1dcc4eb75b"></a>`distribution`: `stove0-nvenc-av1-opus-target`
- <a id="s-eb3989200c"></a>`module`: `stove0_nvenc_av1_opus_target`
- <a id="s-2affd10815"></a>`name`: `contract`
- <a id="s-b0b9f8da46"></a>`owner`: `stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService`
- <a id="s-77058792d7"></a>`unit`: `member`

### Declared structure

- <a id="s-1314459303"></a>`kind`: `"method"`
- <a id="s-f582fb1991"></a>`signature`: `"\"(self) -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [NvencAv1OpusTargetService](stove0-nvenc-av1-opus-target-nvencav1opustargetservice.md)

## Governing policies

- <a id="pa-46e7123033"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-nvenc-av1-opus-target:stove0_nvenc_av1_opus_target](../../../evidence/sources.md#src-b6e7b93ef1) — `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff916a9e20e69134771e6ee7004f851bd684f5ee78f5ff863fe224b523fd2c25 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetContract'\""
  },
  "distribution": "stove0-nvenc-av1-opus-target",
  "module": "stove0_nvenc_av1_opus_target",
  "name": "contract",
  "owner": "stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService",
  "unit": "member"
}
```

</details>
