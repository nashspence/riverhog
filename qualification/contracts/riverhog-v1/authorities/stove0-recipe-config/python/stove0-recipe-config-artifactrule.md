# stove0_recipe_config.ArtifactRule

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-artifactrule:7c77a20c2e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ef82a9c846"></a>
| Field | Shape |
|---|---|
| <a id="s-ac42cd3e07"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-26c0d4cf80"></a>`distribution` | "stove0-recipe-config" |
| <a id="s-3c280dad97"></a>`module` | "stove0_recipe_config" |
| <a id="s-8b914e6399"></a>`name` | "ArtifactRule" |
| <a id="s-1d0dd49bab"></a>`unit` | "export" |

## Governing policies

- <a id="pa-176045925b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.ArtifactRule`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2662438ab3465d0e60fcdca979a8ff973d872b9ba998299c0e59033eabd28121 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5d62e12d3240642997060b77491800bbd94ba5c93d11916162dfafc9c4198945",
    "signature": "\"(*, glob: str = '*', role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)] = 'stove0.source/v1', media_type: str | None = None) -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "ArtifactRule",
  "unit": "export"
}
```
