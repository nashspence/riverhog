# stove0_target_support.TargetService.get_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetservice-get-job:f6f2b21c40 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-df9b044727"></a>
| Field | Shape |
|---|---|
| <a id="s-2d0a817520"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-32ed682a5a"></a>`distribution` | "stove0-target-support" |
| <a id="s-1102b29d8b"></a>`module` | "stove0_target_support" |
| <a id="s-d26240745d"></a>`name` | "get_job" |
| <a id="s-d9146e20c0"></a>`owner` | "stove0_target_support.TargetService" |
| <a id="s-d5a727e4fd"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetService](stove0-target-support-targetservice.md)

## Governing policies

- <a id="pa-7b0720a946"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetService.get_job`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: efc2eaa1ced4e9db10093630259ea6b52a1417f0ba761f95ffca87337c7d77f8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "get_job",
  "owner": "stove0_target_support.TargetService",
  "unit": "member"
}
```
