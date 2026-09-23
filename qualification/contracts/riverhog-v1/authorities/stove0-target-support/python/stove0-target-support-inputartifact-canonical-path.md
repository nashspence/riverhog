# stove0_target_support.InputArtifact.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-inputartifact-canonical-path:af9e5afdd1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eae56b5032"></a>
- <a id="s-ae6eac4171"></a>`distribution`: `stove0-target-support`
- <a id="s-610eb7c98c"></a>`module`: `stove0_target_support`
- <a id="s-c839904aa4"></a>`name`: `canonical_path`
- <a id="s-9966c20d6b"></a>`owner`: `stove0_target_support.InputArtifact`
- <a id="s-c93e031a61"></a>`unit`: `member`

### Declared structure

- <a id="s-e0fc8b8ac9"></a>`kind`: `"classmethod"`
- <a id="s-3e262d89b2"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [InputArtifact](stove0-target-support-inputartifact.md)

## Governing policies

- <a id="pa-76a14b76ad"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.InputArtifact.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d3da1774b7e1dd5ff95f8624f3db57a938446adaa97f425d003c773fd76577f -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "canonical_path",
  "owner": "stove0_target_support.InputArtifact",
  "unit": "member"
}
```

</details>
