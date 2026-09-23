# stove0_target_support.TargetHttpBinding.handle

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targethttpbinding-handle:1a560a9762 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b5e2f12571"></a>
- <a id="s-68320fcb39"></a>`distribution`: `stove0-target-support`
- <a id="s-0fc8b7836f"></a>`module`: `stove0_target_support`
- <a id="s-12b0859d42"></a>`name`: `handle`
- <a id="s-56de72cf95"></a>`owner`: `stove0_target_support.TargetHttpBinding`
- <a id="s-ad31ae3d48"></a>`unit`: `member`

### Declared structure

- <a id="s-ef27126583"></a>`kind`: `"method"`
- <a id="s-f458841ad1"></a>`signature`: `"\"(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'TargetHttpResponse'\""`

## Maintained corroboration

### Related interface records

- [TargetHttpBinding](stove0-target-support-targethttpbinding.md)

## Governing policies

- <a id="pa-e454c789a3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetHttpBinding.handle`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bbf2a16a972a442e86b6d6d11f0dae0ee5c953411925f61b4e97ad37becadcaa -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'TargetHttpResponse'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "handle",
  "owner": "stove0_target_support.TargetHttpBinding",
  "unit": "member"
}
```

</details>
