# stove0_target_support.OutputArtifact.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-outputartifact-canonical-path:ad6322b4c9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-791cd74e36"></a>
- <a id="s-e3137ac6b5"></a>`distribution`: `stove0-target-support`
- <a id="s-9deed0d5c9"></a>`module`: `stove0_target_support`
- <a id="s-78a538db62"></a>`name`: `canonical_path`
- <a id="s-35c23455d5"></a>`owner`: `stove0_target_support.OutputArtifact`
- <a id="s-38d5dda9db"></a>`unit`: `member`

### Declared structure

- <a id="s-01e8ecd713"></a>`kind`: `"classmethod"`
- <a id="s-a5bc38ba93"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [OutputArtifact](stove0-target-support-outputartifact.md)

## Governing policies

- <a id="pa-72e61ab1f3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [reference/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.OutputArtifact.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ffe90355965a90cfcf2220c2fda91c2e2a6c161fabec3b82598496baa5092aa5 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "canonical_path",
  "owner": "stove0_target_support.OutputArtifact",
  "unit": "member"
}
```

</details>
