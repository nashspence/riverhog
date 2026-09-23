# stove0_target_support.PersistentTargetService.get_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-persistenttargetser-321f79f525:fc29bad51d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-97c2c435c2"></a>
- <a id="s-84e1dc39cf"></a>`distribution`: `stove0-target-support`
- <a id="s-3921c31e80"></a>`module`: `stove0_target_support`
- <a id="s-3cd3fd7d84"></a>`name`: `get_job`
- <a id="s-ec32da08e9"></a>`owner`: `stove0_target_support.PersistentTargetService`
- <a id="s-fe477182aa"></a>`unit`: `member`

### Declared structure

- <a id="s-693a0f1607"></a>`kind`: `"method"`
- <a id="s-4a4ccb97e4"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [PersistentTargetService](stove0-target-support-persistenttargetservice.md)

## Governing policies

- <a id="pa-7c64e85d17"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.PersistentTargetService.get_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89b87754e90cabdf0a6cc47fa6046a716173c023584ca0bce4ab9d70fd909e59 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "get_job",
  "owner": "stove0_target_support.PersistentTargetService",
  "unit": "member"
}
```

</details>
