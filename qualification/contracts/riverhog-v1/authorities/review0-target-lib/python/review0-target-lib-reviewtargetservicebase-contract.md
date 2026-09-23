# review0_target_lib.ReviewTargetServiceBase.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-reviewtargetservicebase-contract:3ee4855303 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-446149e663"></a>
- <a id="s-a0c030adab"></a>`distribution`: `review0-target-lib`
- <a id="s-83a0fac605"></a>`module`: `review0_target_lib`
- <a id="s-41f8502271"></a>`name`: `contract`
- <a id="s-1dbe0616a3"></a>`owner`: `review0_target_lib.ReviewTargetServiceBase`
- <a id="s-c2f5afd7a2"></a>`unit`: `member`

### Declared structure

- <a id="s-64dbda469f"></a>`kind`: `"method"`
- <a id="s-a5449ea3f9"></a>`signature`: `"\"(self) -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetServiceBase](review0-target-lib-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-a32e77f91f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.ReviewTargetServiceBase.contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4693a9211c81a4f0966cc804958305e13b77bf694283a6ec7eedd4fbe12cc415 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetContract'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "contract",
  "owner": "review0_target_lib.ReviewTargetServiceBase",
  "unit": "member"
}
```

</details>
