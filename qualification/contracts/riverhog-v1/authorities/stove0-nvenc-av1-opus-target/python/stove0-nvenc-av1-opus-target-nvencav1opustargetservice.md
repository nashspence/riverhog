# stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-nvencav1opus-cae79a63f4:1421f28575 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-db3527de49"></a>
| Field | Shape |
|---|---|
| <a id="s-a2a7c35c23"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-6f1c934427"></a>`distribution` | "stove0-nvenc-av1-opus-target" |
| <a id="s-bd2994f8f4"></a>`module` | "stove0_nvenc_av1_opus_target" |
| <a id="s-9b17a96ff5"></a>`name` | "NvencAv1OpusTargetService" |
| <a id="s-a9643afbc1"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService.preflight](stove0-nvenc-av1-opus-target-nvencav1opustargetservice-preflight.md)

## Governing policies

- <a id="pa-acdb785c16"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-nvenc-av1-opus-target:stove0_nvenc_av1_opus_target](../../../evidence/sources.md#src-b6e7b93ef1) — `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_nvenc_av1_opus_target.NvencAv1OpusTargetService`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 445595b0fb31b34de068634a4e29f49fb16580efd3a9686fcff17c2e06d4cdfc -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_digest: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
  },
  "distribution": "stove0-nvenc-av1-opus-target",
  "module": "stove0_nvenc_av1_opus_target",
  "name": "NvencAv1OpusTargetService",
  "unit": "export"
}
```
