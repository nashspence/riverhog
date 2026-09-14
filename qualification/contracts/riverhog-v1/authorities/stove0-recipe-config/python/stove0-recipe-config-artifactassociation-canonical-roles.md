# stove0_recipe_config.ArtifactAssociation.canonical_roles

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-artifactassociation-ad2b389365:dd7f0e9853 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4630829dca"></a>
- <a id="s-d8dd028084"></a>`distribution`: `stove0-recipe-config`
- <a id="s-90fcb2aa2d"></a>`module`: `stove0_recipe_config`
- <a id="s-dbbd0b6d20"></a>`name`: `canonical_roles`
- <a id="s-66ec0e3d5f"></a>`owner`: `stove0_recipe_config.ArtifactAssociation`
- <a id="s-300731674d"></a>`unit`: `member`

### Declared structure

- <a id="s-93f9d6db79"></a>`kind`: `"classmethod"`
- <a id="s-8f173d50f6"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.ArtifactAssociation](stove0-recipe-config-artifactassociation.md)

## Governing policies

- <a id="pa-709befe742"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.ArtifactAssociation.canonical_roles`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d70fbb44eca1648d169c8dc373294dc9b1c47628c490c9047db194acdae3f92e -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "canonical_roles",
  "owner": "stove0_recipe_config.ArtifactAssociation",
  "unit": "member"
}
```
