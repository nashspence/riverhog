# review0_target_lib.ReviewTargetServiceBase.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-reviewtargetservicebas-95bc14c269:18c71f5520 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9e6aeb9f08"></a>
- <a id="s-a0e02628fb"></a>`distribution`: `review0-target-lib`
- <a id="s-1257adae91"></a>`module`: `review0_target_lib`
- <a id="s-61d00908a8"></a>`name`: `descriptor`
- <a id="s-6631e4fa5f"></a>`owner`: `review0_target_lib.ReviewTargetServiceBase`
- <a id="s-d75a6a8d1e"></a>`unit`: `member`

### Declared structure

- <a id="s-b75ccfa308"></a>`kind`: `"method"`
- <a id="s-11a2c179b0"></a>`signature`: `"\"(self) -> 'TargetDescriptor'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetServiceBase](review0-target-lib-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-46593ddd95"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.ReviewTargetServiceBase.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7415a8756a3681b38f5263deadeba068dfbef0fa7b59b516332460a975b4bc94 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetDescriptor'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "descriptor",
  "owner": "review0_target_lib.ReviewTargetServiceBase",
  "unit": "member"
}
```

</details>
