# stove0_opus_target.OpusTargetService.cancel_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-opus-target:stove0-opus-target-opustargetservice-cancel-job:69510576eb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e8669943b3"></a>
- <a id="s-4010ccd5ff"></a>`distribution`: `stove0-opus-target`
- <a id="s-7fa423481e"></a>`module`: `stove0_opus_target`
- <a id="s-4ddaf2c660"></a>`name`: `cancel_job`
- <a id="s-be46896e19"></a>`owner`: `stove0_opus_target.OpusTargetService`
- <a id="s-7c685e36ac"></a>`unit`: `member`

### Declared structure

- <a id="s-fb05596af2"></a>`kind`: `"method"`
- <a id="s-3d9b53f35a"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [OpusTargetService](stove0-opus-target-opustargetservice.md)

## Governing policies

- <a id="pa-42832c5e5d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-opus-target:stove0_opus_target](../../../evidence/sources/authorities.md#src-9164f15983) — [reference/stove0/targets/opus/target/src/stove0\_opus\_target/\_\_init\_\_.py](../../../../../../reference/stove0/targets/opus/target/src/stove0_opus_target/__init__.py)

### Machine authority

- `/external_contract/python/stove0_opus_target.OpusTargetService.cancel_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 405154095917abe3aba82bc72ba263db89b994560cc7f82cbb06dadea8c1fdcf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-opus-target",
  "module": "stove0_opus_target",
  "name": "cancel_job",
  "owner": "stove0_opus_target.OpusTargetService",
  "unit": "member"
}
```

</details>
