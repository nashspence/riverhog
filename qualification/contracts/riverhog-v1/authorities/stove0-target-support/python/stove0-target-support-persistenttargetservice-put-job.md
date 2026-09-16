# stove0_target_support.PersistentTargetService.put_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-persistenttargetser-60802ca935:59d5163b36 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c6d7c9a080"></a>
- <a id="s-1aad4133b7"></a>`distribution`: `stove0-target-support`
- <a id="s-24294b806e"></a>`module`: `stove0_target_support`
- <a id="s-581c2f8006"></a>`name`: `put_job`
- <a id="s-14cd865d9f"></a>`owner`: `stove0_target_support.PersistentTargetService`
- <a id="s-f1aafe1293"></a>`unit`: `member`

### Declared structure

- <a id="s-2138a10c16"></a>`kind`: `"method"`
- <a id="s-776f0b6a71"></a>`signature`: `"\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [PersistentTargetService](stove0-target-support-persistenttargetservice.md)

## Governing policies

- <a id="pa-d7f886ec4b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.PersistentTargetService.put_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9eb806b109840dacbff1228607dfd70e274ded03f3c1749683c4fa4d8e5f6a2d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "put_job",
  "owner": "stove0_target_support.PersistentTargetService",
  "unit": "member"
}
```

</details>
