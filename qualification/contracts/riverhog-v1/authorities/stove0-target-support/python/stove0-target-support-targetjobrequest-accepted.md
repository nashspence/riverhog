# stove0_target_support.TargetJobRequest.accepted

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetjobrequest-accepted:ac0cca6344 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-60527622a1"></a>
- <a id="s-cdd5cc23a1"></a>`distribution`: `stove0-target-support`
- <a id="s-d98bef5482"></a>`module`: `stove0_target_support`
- <a id="s-95601fb42b"></a>`name`: `accepted`
- <a id="s-12eee095f2"></a>`owner`: `stove0_target_support.TargetJobRequest`
- <a id="s-6fbcdcf501"></a>`unit`: `member`

### Declared structure

- <a id="s-b8d354f4f8"></a>`kind`: `"method"`
- <a id="s-00435eabf9"></a>`signature`: `"\"(self) -> 'AcceptedTargetJob'\""`

## Maintained corroboration

### Related interface records

- [TargetJobRequest](stove0-target-support-targetjobrequest.md)

## Governing policies

- <a id="pa-1ccc722836"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetJobRequest.accepted`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 123b91e75c64914cf994db4017e9116fa5f09a4b7d267ccdf70cac0e8aa9bfbb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'AcceptedTargetJob'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "accepted",
  "owner": "stove0_target_support.TargetJobRequest",
  "unit": "member"
}
```

</details>
