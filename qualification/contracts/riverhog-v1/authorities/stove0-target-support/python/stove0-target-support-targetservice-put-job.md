# stove0_target_support.TargetService.put_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetservice-put-job:b7ffe21ec1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a265a8b011"></a>
- <a id="s-0e4bce158e"></a>`distribution`: `stove0-target-support`
- <a id="s-b1ec337170"></a>`module`: `stove0_target_support`
- <a id="s-017d8f71de"></a>`name`: `put_job`
- <a id="s-a2a4f84e1d"></a>`owner`: `stove0_target_support.TargetService`
- <a id="s-c7241aa841"></a>`unit`: `member`

### Declared structure

- <a id="s-f64c958f31"></a>`kind`: `"method"`
- <a id="s-b69124224b"></a>`signature`: `"\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetService](stove0-target-support-targetservice.md)

## Governing policies

- <a id="pa-03a3c69842"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetService.put_job`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 48081d81c55d9666684a9a7b9370656241d016d7bcd8ae829a20851d15890ee1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "put_job",
  "owner": "stove0_target_support.TargetService",
  "unit": "member"
}
```
