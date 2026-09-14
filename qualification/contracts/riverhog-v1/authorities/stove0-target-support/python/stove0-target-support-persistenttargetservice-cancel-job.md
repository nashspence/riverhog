# stove0_target_support.PersistentTargetService.cancel_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-persistenttargetser-26c8083159:a13c92cf1f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-413676c6d6"></a>
- <a id="s-f635640c6f"></a>`distribution`: `stove0-target-support`
- <a id="s-e2fa947605"></a>`module`: `stove0_target_support`
- <a id="s-78bdf517bc"></a>`name`: `cancel_job`
- <a id="s-0def453b5b"></a>`owner`: `stove0_target_support.PersistentTargetService`
- <a id="s-54cdac881b"></a>`unit`: `member`

### Declared structure

- <a id="s-263d5b0324"></a>`kind`: `"method"`
- <a id="s-2554db0a99"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_support.PersistentTargetService](stove0-target-support-persistenttargetservice.md)

## Governing policies

- <a id="pa-cca11e0832"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.PersistentTargetService.cancel_job`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7d109d96cdc12b264711716c37af5f1c055c6f9721b8d116cfa3df5430ea4d8f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "cancel_job",
  "owner": "stove0_target_support.PersistentTargetService",
  "unit": "member"
}
```
