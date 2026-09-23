# review0_target_lib.ReviewTargetServiceBase.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-reviewtargetservicebase-close:a804e22eb6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-24c566a2c9"></a>
- <a id="s-e04eea9b5c"></a>`distribution`: `review0-target-lib`
- <a id="s-60341d1673"></a>`module`: `review0_target_lib`
- <a id="s-05e806e3b7"></a>`name`: `close`
- <a id="s-4b0f363a53"></a>`owner`: `review0_target_lib.ReviewTargetServiceBase`
- <a id="s-2722a620a1"></a>`unit`: `member`

### Declared structure

- <a id="s-cf23b70d4c"></a>`kind`: `"method"`
- <a id="s-0e14e58c87"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetServiceBase](review0-target-lib-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-fa7138c2f4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.ReviewTargetServiceBase.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a0fef0c72380f4319db5f14e8d6a4853342a6af414e37f79f1c946362a60299 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "close",
  "owner": "review0_target_lib.ReviewTargetServiceBase",
  "unit": "member"
}
```

</details>
