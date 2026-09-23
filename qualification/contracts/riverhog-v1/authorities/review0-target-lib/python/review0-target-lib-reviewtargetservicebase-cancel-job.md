# review0_target_lib.ReviewTargetServiceBase.cancel_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-reviewtargetservicebas-69d897d2a4:bd2165f1d0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fad2d7f2a3"></a>
- <a id="s-c90fa32805"></a>`distribution`: `review0-target-lib`
- <a id="s-b8f9baac8f"></a>`module`: `review0_target_lib`
- <a id="s-aa2f6a86c3"></a>`name`: `cancel_job`
- <a id="s-2b0e0d4cf1"></a>`owner`: `review0_target_lib.ReviewTargetServiceBase`
- <a id="s-5484275bec"></a>`unit`: `member`

### Declared structure

- <a id="s-7a2f8d3b21"></a>`kind`: `"method"`
- <a id="s-d70f63bf19"></a>`signature`: `"\"(self, job_id: 'str') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetServiceBase](review0-target-lib-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-4d22e00b07"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.ReviewTargetServiceBase.cancel_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0d62c89e8fa57aa227a47d326e24fa80cc4158b51baefa5a0c9a7aab7d607c6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'TargetJobStatus'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "cancel_job",
  "owner": "review0_target_lib.ReviewTargetServiceBase",
  "unit": "member"
}
```

</details>
