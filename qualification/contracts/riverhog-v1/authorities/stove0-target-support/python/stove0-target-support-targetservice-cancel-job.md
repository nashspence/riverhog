# stove0_target_support.TargetService.cancel_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetservice-cancel-job:a925530daf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7ce9b76e44"></a>
- <a id="s-e2cc963058"></a>`distribution`: `stove0-target-support`
- <a id="s-01c3b6d51f"></a>`module`: `stove0_target_support`
- <a id="s-b0e29a10a1"></a>`name`: `cancel_job`
- <a id="s-67d508bd66"></a>`owner`: `stove0_target_support.TargetService`
- <a id="s-ee9cf2fd3a"></a>`unit`: `member`

### Declared structure

- <a id="s-410f829789"></a>`kind`: `"method"`
- <a id="s-747724a1aa"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [TargetService](stove0-target-support-targetservice.md)

## Governing policies

- <a id="pa-355342ad3e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetService.cancel_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e872238c90401e5313a246720af533c52a06ac2ae3eaa367abb87a485ca5b9df -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "cancel_job",
  "owner": "stove0_target_support.TargetService",
  "unit": "member"
}
```

</details>
