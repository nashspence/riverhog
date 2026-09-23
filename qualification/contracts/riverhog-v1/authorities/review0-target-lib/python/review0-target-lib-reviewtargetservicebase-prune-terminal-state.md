# review0_target_lib.ReviewTargetServiceBase.prune_terminal_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-reviewtargetservicebas-9843182a8d:7d29e44389 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4aa5ec0778"></a>
- <a id="s-32b0d99b91"></a>`distribution`: `review0-target-lib`
- <a id="s-ff27691cec"></a>`module`: `review0_target_lib`
- <a id="s-9090fa2c15"></a>`name`: `prune_terminal_state`
- <a id="s-8b4ec8948a"></a>`owner`: `review0_target_lib.ReviewTargetServiceBase`
- <a id="s-fb04041c65"></a>`unit`: `member`

### Declared structure

- <a id="s-d87592c787"></a>`kind`: `"method"`
- <a id="s-43dac58c90"></a>`signature`: `"\"(self, *, now: 'float \| None' = None) -> 'dict[str, int]'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetServiceBase](review0-target-lib-reviewtargetservicebase.md)

## Governing policies

- <a id="pa-6b18c084a0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.ReviewTargetServiceBase.prune_terminal_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 50d8563a234d8d0ef8d5014539e3db216f5375406acca3e632399eb5437ac37b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, now: 'float | None' = None) -> 'dict[str, int]'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "prune_terminal_state",
  "owner": "review0_target_lib.ReviewTargetServiceBase",
  "unit": "member"
}
```

</details>
