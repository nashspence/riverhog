# stove0_target_support.TargetDescriptor.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetdescriptor-verify-digest:b76066a74c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a318e01a26"></a>
- <a id="s-31d5e0ad37"></a>`distribution`: `stove0-target-support`
- <a id="s-c62aaa4feb"></a>`module`: `stove0_target_support`
- <a id="s-a7e6f75b32"></a>`name`: `verify_digest`
- <a id="s-cddeb54909"></a>`owner`: `stove0_target_support.TargetDescriptor`
- <a id="s-810fcdff8f"></a>`unit`: `member`

### Declared structure

- <a id="s-55a9b92098"></a>`kind`: `"method"`
- <a id="s-1b802edf87"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TargetDescriptor](stove0-target-support-targetdescriptor.md)

## Governing policies

- <a id="pa-3b5dd041e0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetDescriptor.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c548da5f9b0da35ebe7e79f28dd7b3fae9b4c7bd765f0c4c75486098b3105735 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "verify_digest",
  "owner": "stove0_target_support.TargetDescriptor",
  "unit": "member"
}
```

</details>
