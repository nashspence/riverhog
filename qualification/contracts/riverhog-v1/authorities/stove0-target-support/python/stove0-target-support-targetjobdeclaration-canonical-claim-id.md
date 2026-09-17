# stove0_target_support.TargetJobDeclaration.canonical_claim_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetjobdeclaratio-74a12a302d:73864d988d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2a766848c2"></a>
- <a id="s-4e9791892b"></a>`distribution`: `stove0-target-support`
- <a id="s-4282e54946"></a>`module`: `stove0_target_support`
- <a id="s-a4edd4b600"></a>`name`: `canonical_claim_id`
- <a id="s-4e5fe580db"></a>`owner`: `stove0_target_support.TargetJobDeclaration`
- <a id="s-a543077ca2"></a>`unit`: `member`

### Declared structure

- <a id="s-09af1d104b"></a>`kind`: `"classmethod"`
- <a id="s-3871bfea53"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [TargetJobDeclaration](stove0-target-support-targetjobdeclaration.md)

## Governing policies

- <a id="pa-d08ae0542b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetJobDeclaration.canonical_claim_id`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61529feaf2f95fc3c317558fd2cb134de67172c1be9f916feb87f5c30b3b8e4a -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "canonical_claim_id",
  "owner": "stove0_target_support.TargetJobDeclaration",
  "unit": "member"
}
```

</details>
