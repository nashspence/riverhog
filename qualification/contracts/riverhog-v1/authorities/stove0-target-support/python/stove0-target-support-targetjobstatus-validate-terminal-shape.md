# stove0_target_support.TargetJobStatus.validate_terminal_shape

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetjobstatus-val-575f7fb072:82415edbb1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-286c9a8f37"></a>
- <a id="s-323794b77f"></a>`distribution`: `stove0-target-support`
- <a id="s-97f70e7a38"></a>`module`: `stove0_target_support`
- <a id="s-89e108e291"></a>`name`: `validate_terminal_shape`
- <a id="s-29ec19c312"></a>`owner`: `stove0_target_support.TargetJobStatus`
- <a id="s-ccd10d2af1"></a>`unit`: `member`

### Declared structure

- <a id="s-1722721843"></a>`kind`: `"method"`
- <a id="s-ccc2222d02"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TargetJobStatus](stove0-target-support-targetjobstatus.md)

## Governing policies

- <a id="pa-053153d976"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetJobStatus.validate_terminal_shape`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de16c07923e2a40f198c5f2671c5a6f3a2ed70eb531c57098ae343178a8667cb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "validate_terminal_shape",
  "owner": "stove0_target_support.TargetJobStatus",
  "unit": "member"
}
```

</details>
