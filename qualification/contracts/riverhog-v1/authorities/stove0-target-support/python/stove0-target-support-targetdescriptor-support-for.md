# stove0_target_support.TargetDescriptor.support_for

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetdescriptor-support-for:0649b84d56 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-55ee1d6dff"></a>
- <a id="s-e82a22ebe7"></a>`distribution`: `stove0-target-support`
- <a id="s-432406a0a8"></a>`module`: `stove0_target_support`
- <a id="s-7ae25fdbbe"></a>`name`: `support_for`
- <a id="s-df52da176f"></a>`owner`: `stove0_target_support.TargetDescriptor`
- <a id="s-f0b3c25a66"></a>`unit`: `member`

### Declared structure

- <a id="s-5846bcd5c2"></a>`kind`: `"method"`
- <a id="s-5de273da79"></a>`signature`: `"\"(self, operation_id: 'str') -> 'TargetOperationSupport'\""`

## Maintained corroboration

### Related interface records

- [TargetDescriptor](stove0-target-support-targetdescriptor.md)

## Governing policies

- <a id="pa-a7f25378f8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetDescriptor.support_for`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e95ecd80b472b5a0962f3636223e4985de9850b804256bb50dbbc482a29c091c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, operation_id: 'str') -> 'TargetOperationSupport'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "support_for",
  "owner": "stove0_target_support.TargetDescriptor",
  "unit": "member"
}
```

</details>
