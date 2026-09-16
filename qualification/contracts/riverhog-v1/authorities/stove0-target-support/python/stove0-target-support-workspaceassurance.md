# stove0_target_support.WorkspaceAssurance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-workspaceassurance:93e5b70fbf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dca829863b"></a>
- <a id="s-391480b556"></a>`distribution`: `stove0-target-support`
- <a id="s-a6a4e1e213"></a>`module`: `stove0_target_support`
- <a id="s-2cf7af56b3"></a>`name`: `WorkspaceAssurance`
- <a id="s-c0033e0773"></a>`unit`: `export`

### Declared structure

- <a id="s-af449b83a3"></a>`kind`: `"object"`
- <a id="s-555cbd9075"></a>`type`: `"typing._LiteralGenericAlias"`

## Governing policies

- <a id="pa-f8a6dd17b3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.WorkspaceAssurance`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e32b9e874ebfea1a363011254646a023909519ce14b990e292b453e4923c3b1 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "WorkspaceAssurance",
  "unit": "export"
}
```

</details>
