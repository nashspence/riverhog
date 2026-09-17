# stove0_core.RecipeDefinition.canonical_members

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipedefinition-canonical-members:4e35c25ff3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e5e9dc8a8d"></a>
- <a id="s-4d98ea2c8e"></a>`distribution`: `stove0-server`
- <a id="s-f1c9f3d3f1"></a>`module`: `stove0_core`
- <a id="s-dda01b0f11"></a>`name`: `canonical_members`
- <a id="s-0f72e76619"></a>`owner`: `stove0_core.RecipeDefinition`
- <a id="s-9eaf7752e5"></a>`unit`: `member`

### Declared structure

- <a id="s-db482ac66c"></a>`kind`: `"method"`
- <a id="s-c9468bf070"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [RecipeDefinition](stove0-core-recipedefinition.md)

## Governing policies

- <a id="pa-35f663b012"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RecipeDefinition.canonical_members`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e32dd1663b897ae50eb9f2b61fb630ed68ad0ec3621d43e07a5bbddb08b2c791 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "canonical_members",
  "owner": "stove0_core.RecipeDefinition",
  "unit": "member"
}
```

</details>
