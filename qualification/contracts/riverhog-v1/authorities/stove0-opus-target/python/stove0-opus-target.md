# stove0_opus_target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-opus-target:stove0-opus-target:c1ccc6b2a0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-bc9514dc2b"></a>
| Field | Shape |
|---|---|
| <a id="s-8dd7601fe3"></a>`candidate_id` | "python:stove0-opus-target:stove0_opus_target" |
| <a id="s-1916dd8795"></a>`distribution` | "stove0-opus-target" |
| <a id="s-4a309a19f4"></a>`exports` | additional keys=`OpusTargetService` |
| <a id="s-6f2535e75e"></a>`module` | "stove0_opus_target" |

## Governing policies

- <a id="pa-fd137bda4b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-opus-target:stove0_opus_target](../../../evidence/sources.md#src-9164f15983) — `reference/stove0/targets/opus/target/src/stove0_opus_target/__init__.py`

### Machine authority

- `/external_contract/python/46`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ec8e0ab42ab223ef0a769b35efc0d536e646ffc6322c2406104e5c55c06657c3 -->

```json
{
  "candidate_id": "python:stove0-opus-target:stove0_opus_target",
  "distribution": "stove0-opus-target",
  "exports": {
    "OpusTargetService": {
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
  "module": "stove0_opus_target"
}
```
