# stove0_core.RecipeDefinition.ref

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipedefinition-ref:6f5318df3b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-79c044249a"></a>
| Field | Shape |
|---|---|
| <a id="s-189209342f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-752ec7e183"></a>`distribution` | "stove0-server" |
| <a id="s-df9ebdf29b"></a>`module` | "stove0_core" |
| <a id="s-754f26a083"></a>`name` | "ref" |
| <a id="s-da47d948f7"></a>`owner` | "stove0_core.RecipeDefinition" |
| <a id="s-dd013d07e0"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.RecipeDefinition](stove0-core-recipedefinition.md)

## Governing policies

- <a id="pa-58fadf3049"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RecipeDefinition.ref`

### Exact owned JSON

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
