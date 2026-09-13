# stove0_core.RecipeDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipedefinition:0edcbcb7e7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-45957da6ae"></a>
| Field | Shape |
|---|---|
| <a id="s-846ba1f4d7"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-9e4b232672"></a>`distribution` | "stove0-server" |
| <a id="s-a611db797e"></a>`module` | "stove0_core" |
| <a id="s-d1b48c83db"></a>`name` | "RecipeDefinition" |
| <a id="s-3f1f7951bc"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_core.RecipeDefinition.canonical_members](stove0-core-recipedefinition-canonical-members.md)
- [stove0_core.RecipeDefinition.identity_document](stove0-core-recipedefinition-identity-document.md)
- [stove0_core.RecipeDefinition.ref](stove0-core-recipedefinition-ref.md)
- [stove0_core.RecipeDefinition.sha256](stove0-core-recipedefinition-sha256.md)

## Governing policies

- <a id="pa-68986ec42f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RecipeDefinition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8df082a8ddbadbfd51b5fcd933c9ce163ff23503c4dd64b1661f18bb8be6d298 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6c28d2c8a41f504854067b2123a03ac0343f7c20f4281141a0871f4e6e4deb50",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], revision: Annotated[int, Ge(ge=1)], event_input_closure: Literal['single-finalized-collection'] = 'single-finalized-collection', artifact_associations: tuple[stove0_recipe_config.models.ArtifactAssociation, ...] = (), observers: tuple[stove0_recipe_config.models.ObserverUse, ...] = (), routes: Annotated[tuple[Annotated[stove0_recipe_config.models.RecipeRoute | stove0_recipe_config.models.RecipeCoordinationRoute, FieldInfo(annotation=NoneType, required=True, discriminator='kind')], ...], MinLen(min_length=1)], unmatched_artifact_disposition: Literal['retain-in-source', 'reject-work'], allow_derived_inputs: bool = False, source_retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, join: stove0_recipe_config.models.RecipeJoin | None = None) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "RecipeDefinition",
  "unit": "export"
}
```
