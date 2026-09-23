# review0_target_lib.ReviewTargetServiceBase.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-reviewtargetservicebase-preflight:8c84d8c055 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8be0e18ad4"></a>
- <a id="s-28ffcb2fe1"></a>`distribution`: `review0-target-lib`
- <a id="s-fe7e7f857a"></a>`module`: `review0_target_lib`
- <a id="s-bc50f4430b"></a>`name`: `preflight`
- <a id="s-22a7bd14aa"></a>`owner`: `review0_target_lib.ReviewTargetServiceBase`
- <a id="s-5834d5d5a8"></a>`unit`: `member`

### Declared structure

- <a id="s-e9efcf4d87"></a>`kind`: `"method"`
- <a id="s-ab340a5557"></a>`signature`: `"\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetServiceBase](review0-target-lib-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-b91a8bb098"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.ReviewTargetServiceBase.preflight`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7f166aa07d07c93d62900485978bc19b5967317d4bb7cb499d8bb808458c9f65 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "preflight",
  "owner": "review0_target_lib.ReviewTargetServiceBase",
  "unit": "member"
}
```

</details>
