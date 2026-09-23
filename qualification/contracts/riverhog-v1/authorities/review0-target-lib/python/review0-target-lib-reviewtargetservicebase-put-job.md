# review0_target_lib.ReviewTargetServiceBase.put_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-reviewtargetservicebase-put-job:f9d724def8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-443b7bf7a5"></a>
- <a id="s-93e6d97a00"></a>`distribution`: `review0-target-lib`
- <a id="s-4ef28c151f"></a>`module`: `review0_target_lib`
- <a id="s-c55c4b7614"></a>`name`: `put_job`
- <a id="s-c9a73a5b6f"></a>`owner`: `review0_target_lib.ReviewTargetServiceBase`
- <a id="s-09f6384f08"></a>`unit`: `member`

### Declared structure

- <a id="s-f184222967"></a>`kind`: `"method"`
- <a id="s-03271a6472"></a>`signature`: `"\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetServiceBase](review0-target-lib-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-a672801850"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.ReviewTargetServiceBase.put_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a661a118083e40846ad71640af1be60c37fa3fcc57f53fbee857ac14cc9fce0b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetJobRequest') -> 'TargetJobStatus'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "put_job",
  "owner": "review0_target_lib.ReviewTargetServiceBase",
  "unit": "member"
}
```

</details>
