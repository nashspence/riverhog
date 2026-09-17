# stove0_core.RecipeDefinition.sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipedefinition-sha256:8b0929d70b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0f64476f94"></a>
- <a id="s-1a057462c0"></a>`distribution`: `stove0-server`
- <a id="s-0fe404cb28"></a>`module`: `stove0_core`
- <a id="s-4274322b1f"></a>`name`: `sha256`
- <a id="s-8efc32656a"></a>`owner`: `stove0_core.RecipeDefinition`
- <a id="s-a9601e69fb"></a>`unit`: `member`

### Declared structure

- <a id="s-f3c161b4a2"></a>`kind`: `"property"`
- <a id="s-d4cb382ee8"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [RecipeDefinition](stove0-core-recipedefinition.md)

## Governing policies

- <a id="pa-8bf60d1160"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RecipeDefinition.sha256`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 11472c571d12399efbf45eac277b2861ad71f7f4f1f8fc9365f9eb3679b89cbb -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "sha256",
  "owner": "stove0_core.RecipeDefinition",
  "unit": "member"
}
```

</details>
