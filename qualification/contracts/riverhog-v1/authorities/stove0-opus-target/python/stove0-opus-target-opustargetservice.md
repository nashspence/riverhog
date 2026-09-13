# stove0_opus_target.OpusTargetService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-opus-target:stove0-opus-target-opustargetservice:73e0383b03 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b69dbaee7"></a>
| Field | Shape |
|---|---|
| <a id="s-08a9407a72"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ea48b69c3f"></a>`distribution` | "stove0-opus-target" |
| <a id="s-718414f4aa"></a>`module` | "stove0_opus_target" |
| <a id="s-837dee7030"></a>`name` | "OpusTargetService" |
| <a id="s-a7fcef65cb"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_opus_target.OpusTargetService.preflight](stove0-opus-target-opustargetservice-preflight.md)

## Governing policies

- <a id="pa-172e6114f5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-opus-target:stove0_opus_target](../../../evidence/sources.md#src-9164f15983) — `reference/stove0/targets/opus/target/src/stove0_opus_target/__init__.py`

### Machine authority

- `/external_contract/python/stove0_opus_target.OpusTargetService`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2c902add701b48207e35388def5f1ab6e2576674efca907cf87c6ed0191e7aaf -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', ffmpeg: 'str' = 'ffmpeg', source_revision: 'str' = 'unknown', image_digest: 'str', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
  },
  "distribution": "stove0-opus-target",
  "module": "stove0_opus_target",
  "name": "OpusTargetService",
  "unit": "export"
}
```
