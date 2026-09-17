# stove0_target_support.TargetJobRequest.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetjobrequest-verify-digest:54ae493ee6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-67036d46ca"></a>
- <a id="s-e8013b796a"></a>`distribution`: `stove0-target-support`
- <a id="s-b52026b312"></a>`module`: `stove0_target_support`
- <a id="s-5adafd027e"></a>`name`: `verify_digest`
- <a id="s-60ca5bce7e"></a>`owner`: `stove0_target_support.TargetJobRequest`
- <a id="s-7291adaaa0"></a>`unit`: `member`

### Declared structure

- <a id="s-51773682fc"></a>`kind`: `"method"`
- <a id="s-27bd63b70b"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TargetJobRequest](stove0-target-support-targetjobrequest.md)

## Governing policies

- <a id="pa-c20e5afcd0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetJobRequest.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e7fdae8db693347b68328c10e9220842c64f40cb3128b1c6ed678b46bdc5ec91 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "verify_digest",
  "owner": "stove0_target_support.TargetJobRequest",
  "unit": "member"
}
```

</details>
