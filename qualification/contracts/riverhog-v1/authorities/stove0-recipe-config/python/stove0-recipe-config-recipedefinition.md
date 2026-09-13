# stove0_recipe_config.RecipeDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipedefinition:c7c878259b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0119d3013a"></a>
| Field | Shape |
|---|---|
| <a id="s-f265977786"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-e4c3252c02"></a>`distribution` | "stove0-recipe-config" |
| <a id="s-20906aff87"></a>`module` | "stove0_recipe_config" |
| <a id="s-805f9f2ed8"></a>`name` | "RecipeDefinition" |
| <a id="s-41370f7180"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_recipe_config.RecipeDefinition.canonical_members](stove0-recipe-config-recipedefinition-canonical-members.md)
- [stove0_recipe_config.RecipeDefinition.identity_document](stove0-recipe-config-recipedefinition-identity-document.md)
- [stove0_recipe_config.RecipeDefinition.ref](stove0-recipe-config-recipedefinition-ref.md)
- [stove0_recipe_config.RecipeDefinition.sha256](stove0-recipe-config-recipedefinition-sha256.md)

## Governing policies

- <a id="pa-f994c22696"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeDefinition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a121eddafe26e68673d0fe8752404db27866b33dbb743debad2b8440bd67824d -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6c28d2c8a41f504854067b2123a03ac0343f7c20f4281141a0871f4e6e4deb50",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], revision: Annotated[int, Ge(ge=1)], event_input_closure: Literal['single-finalized-collection'] = 'single-finalized-collection', artifact_associations: tuple[stove0_recipe_config.models.ArtifactAssociation, ...] = (), observers: tuple[stove0_recipe_config.models.ObserverUse, ...] = (), routes: Annotated[tuple[Annotated[stove0_recipe_config.models.RecipeRoute | stove0_recipe_config.models.RecipeCoordinationRoute, FieldInfo(annotation=NoneType, required=True, discriminator='kind')], ...], MinLen(min_length=1)], unmatched_artifact_disposition: Literal['retain-in-source', 'reject-work'], allow_derived_inputs: bool = False, source_retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, join: stove0_recipe_config.models.RecipeJoin | None = None) -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "RecipeDefinition",
  "unit": "export"
}
```
