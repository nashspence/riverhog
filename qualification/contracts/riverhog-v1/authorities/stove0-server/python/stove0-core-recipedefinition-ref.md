# stove0_core.RecipeDefinition.ref

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipedefinition-ref:6f5318df3b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-79c044249a"></a>
- <a id="s-752ec7e183"></a>`distribution`: `stove0-server`
- <a id="s-df9ebdf29b"></a>`module`: `stove0_core`
- <a id="s-754f26a083"></a>`name`: `ref`
- <a id="s-da47d948f7"></a>`owner`: `stove0_core.RecipeDefinition`
- <a id="s-dd013d07e0"></a>`unit`: `member`

### Declared structure

- <a id="s-6a1fee984e"></a>`kind`: `"property"`
- <a id="s-4d71951e77"></a>`signature`: `"\"(self) -> 'RecipeRef'\""`

## Maintained corroboration

### Related interface records

- [RecipeDefinition](stove0-core-recipedefinition.md)

## Governing policies

- <a id="pa-58fadf3049"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RecipeDefinition.ref`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2bd7d18abf360d3fd7bf1cbb1643932a5c2167ffba15646bfeede31a66d60f30 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'RecipeRef'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ref",
  "owner": "stove0_core.RecipeDefinition",
  "unit": "member"
}
```

</details>
