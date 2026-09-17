# stove0_target_support.OutputArtifactContract.unique_roles

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-outputartifactcontr-2015f6df10:d4bfe83972 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4c6df3dd86"></a>
- <a id="s-be21da4b43"></a>`distribution`: `stove0-target-support`
- <a id="s-7d24203a99"></a>`module`: `stove0_target_support`
- <a id="s-a34a191412"></a>`name`: `unique_roles`
- <a id="s-36560fdd48"></a>`owner`: `stove0_target_support.OutputArtifactContract`
- <a id="s-292abe7baf"></a>`unit`: `member`

### Declared structure

- <a id="s-976c7a8025"></a>`kind`: `"classmethod"`
- <a id="s-74969d2b42"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [OutputArtifactContract](stove0-target-support-outputartifactcontract.md)

## Governing policies

- <a id="pa-050c98520c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.OutputArtifactContract.unique_roles`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ec08a52850c83f16286a5db59395a7e6683537c73fa8c037b694157aef654347 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "unique_roles",
  "owner": "stove0_target_support.OutputArtifactContract",
  "unit": "member"
}
```

</details>
