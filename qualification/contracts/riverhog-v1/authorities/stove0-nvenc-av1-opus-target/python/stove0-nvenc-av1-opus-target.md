# stove0_nvenc_av1_opus_target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target:6b325971aa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-0ac25d16aa) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7a07566626"></a>
| Field | Shape |
|---|---|
| <a id="s-731044eaf3"></a>`candidate_id` | "python:stove0-nvenc-av1-opus-target:stove0_nvenc_av1_opus_target" |
| <a id="s-b2e487e688"></a>`distribution` | "stove0-nvenc-av1-opus-target" |
| <a id="s-1a8c15c291"></a>`exports` | additional keys=`NvencAv1OpusTargetService` |
| <a id="s-637c0bd105"></a>`module` | "stove0_nvenc_av1_opus_target" |

## Governing policies

- <a id="pa-14dcce426a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-nvenc-av1-opus-target:stove0_nvenc_av1_opus_target](../../../evidence/sources.md#src-b6e7b93ef1) — `reference/stove0/targets/nvenc-av1-opus/target/src/stove0_nvenc_av1_opus_target/__init__.py`

### Machine authority

- `/external_contract/python/40`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59ed8af9869a99f1d4fd2702d3a0e9ed8be779caf7cd7689a7340fa70daa436f -->

```json
{
  "candidate_id": "python:stove0-nvenc-av1-opus-target:stove0_nvenc_av1_opus_target",
  "distribution": "stove0-nvenc-av1-opus-target",
  "exports": {
    "NvencAv1OpusTargetService": {
      "kind": "class",
      "members": {
        "preflight": {
          "kind": "method",
          "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
        }
      },
      "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_digest: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
    }
  },
  "module": "stove0_nvenc_av1_opus_target"
}
```
