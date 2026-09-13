# stove0_recipe_config

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config:692d207b68 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9a3ee044e2"></a>
| Field | Shape |
|---|---|
| <a id="s-47f805391a"></a>`candidate_id` | "python:stove0-recipe-config:stove0_recipe_config" |
| <a id="s-130e7eb5cd"></a>`distribution` | "stove0-recipe-config" |
| <a id="s-d11603df95"></a>`exports` | additional keys=`ArtifactAssociation`, `ArtifactFactBinding`, `ArtifactRule`, `FactPredicate`, `ObserverUse`, `OperationProjection`, `RecipeBranch`, `RecipeCatalog`, `RecipeCoordinationRoute`, `RecipeDefinition`, `RecipeJoin`, `RecipeJoinMember`, `RecipeRoute` |
| <a id="s-6997682046"></a>`module` | "stove0_recipe_config" |

## Governing policies

- <a id="pa-7454cb7979"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources.md#src-9e1422d2d6) — `reference/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py`

### Machine authority

- `/external_contract/python/48`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ab102a235b093ecdf14a18948ca8b77d6534c64fdcffae24587c75906f8fb28f -->

```json
{
  "candidate_id": "python:stove0-recipe-config:stove0_recipe_config",
  "distribution": "stove0-recipe-config",
  "exports": {
    "ArtifactAssociation": {
      "kind": "class",
      "members": {
        "canonical_roles": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
        }
      },
      "schema_sha256": "00a5b3ff7e9d421eedaa499625122b72ab475c426ae33f3193ca4bb26500ed7d",
      "signature": "\"(*, primary_role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], associated_roles: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)], path_identity: Literal['same-parent-stem'] = 'same-parent-stem') -> None\""
    },
    "ArtifactFactBinding": {
      "kind": "class",
      "schema_sha256": "be2e042f0f939418ce250f41c4b0fb8be5f7c53668a50eff8f53679cc92e6567",
      "signature": "\"(*, records_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')], artifact_id_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')] = '/artifact_id') -> None\""
    },
    "ArtifactRule": {
      "kind": "class",
      "schema_sha256": "5d62e12d3240642997060b77491800bbd94ba5c93d11916162dfafc9c4198945",
      "signature": "\"(*, glob: str = '*', role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)] = 'stove0.source/v1', media_type: str | None = None) -> None\""
    },
    "FactPredicate": {
      "kind": "class",
      "members": {
        "valid_scope": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "de5a579067305377d33e95cc6b3c461652ad1ef30db493109919896dab179fa4",
      "signature": "\"(*, observation_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], artifact_roles: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...] = (), artifact_facts: stove0_recipe_config.models.ArtifactFactBinding | None = None, pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')], operator: Literal['equals', 'not-equals', 'contains', 'exists'] = 'equals', value: JsonValue = None) -> None\""
    },
    "ObserverUse": {
      "kind": "class",
      "schema_sha256": "299564b518338751fdc31ca137e652dea4f73f0c34d5a19ff14139d415124db1",
      "signature": "\"(*, registration_id: str, contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], artifact_rules: tuple[stove0_recipe_config.models.ArtifactRule, ...] = (ArtifactRule(glob='*', role='stove0.source/v1', media_type=None),), options: dict[str, JsonValue] = <factory>, timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)] = 300, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576, retrieval_policy: Literal['available-only', 'allow'] = 'available-only') -> None\""
    },
    "OperationProjection": {
      "kind": "class",
      "schema_sha256": "412cc6fe371170daf347f525d51353e79834de6222531002457e205e033c56fa",
      "signature": "\"(*, source: Literal['work-effective-intent', 'work-evaluation'], source_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')], destination: Literal['intent', 'target-options'], destination_pointer: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$')]) -> None\""
    },
    "RecipeBranch": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "RecipeCatalog": {
      "kind": "class",
      "members": {
        "load": {
          "kind": "classmethod",
          "signature": "\"(cls, path: 'Path') -> 'RecipeCatalog'\""
        },
        "operation": {
          "kind": "method",
          "signature": "\"(self, operation_id: 'str') -> 'OperationContract'\""
        },
        "recipe": {
          "kind": "method",
          "signature": "\"(self, recipe_id: 'str', revision: 'int | None' = None) -> 'RecipeDefinition'\""
        },
        "sha256": {
          "kind": "property",
          "signature": "\"(self) -> 'str'\""
        },
        "valid_catalog": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        },
        "validation_document": {
          "kind": "method",
          "signature": "\"(self) -> 'dict[str, JsonValue]'\""
        }
      },
      "schema_sha256": "d917d4c47bebfc8abcea36832e60bdf80ca2ac7cd717d0f30305b3d377409fd5",
      "signature": "\"(*, format: Literal['stove0-recipes/v1'] = 'stove0-recipes/v1', operations: tuple[stove0_target_protocol.protocol.OperationContract, ...], recipes: tuple[stove0_recipe_config.models.RecipeDefinition, ...]) -> None\""
    },
    "RecipeCoordinationRoute": {
      "kind": "class",
      "members": {
        "coordination_projections_target_intent_only": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "7c52772dd554a9f5ee79ef94c53b55f649cf8904cadd74bc0cebcf7f950ec1b4",
      "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], when: tuple[stove0_recipe_config.models.FactPredicate, ...] = (), artifact_rules: tuple[stove0_recipe_config.models.ArtifactRule, ...] = (ArtifactRule(glob='*', role='stove0.source/v1', media_type=None),), primary_role: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)]] = None, associated_roles: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...] = (), intent: dict[str, JsonValue] = <factory>, projections: tuple[stove0_recipe_config.models.OperationProjection, ...] = (), kind: Literal['coordination'] = 'coordination', recipe: stove0_protocol.models.RecipeRef) -> None\""
    },
    "RecipeDefinition": {
      "kind": "class",
      "members": {
        "canonical_members": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        },
        "identity_document": {
          "kind": "method",
          "signature": "\"(self) -> 'dict[str, JsonValue]'\""
        },
        "ref": {
          "kind": "property",
          "signature": "\"(self) -> 'RecipeRef'\""
        },
        "sha256": {
          "kind": "property",
          "signature": "\"(self) -> 'str'\""
        }
      },
      "schema_sha256": "6c28d2c8a41f504854067b2123a03ac0343f7c20f4281141a0871f4e6e4deb50",
      "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], revision: Annotated[int, Ge(ge=1)], event_input_closure: Literal['single-finalized-collection'] = 'single-finalized-collection', artifact_associations: tuple[stove0_recipe_config.models.ArtifactAssociation, ...] = (), observers: tuple[stove0_recipe_config.models.ObserverUse, ...] = (), routes: Annotated[tuple[Annotated[stove0_recipe_config.models.RecipeRoute | stove0_recipe_config.models.RecipeCoordinationRoute, FieldInfo(annotation=NoneType, required=True, discriminator='kind')], ...], MinLen(min_length=1)], unmatched_artifact_disposition: Literal['retain-in-source', 'reject-work'], allow_derived_inputs: bool = False, source_retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, join: stove0_recipe_config.models.RecipeJoin | None = None) -> None\""
    },
    "RecipeJoin": {
      "kind": "class",
      "members": {
        "canonical_projections": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "5fef83beb5dcd069ffebfd5201605f3b9a08ae25dab3e7e9504bf4b01666daf8",
      "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], members: Annotated[tuple[stove0_recipe_config.models.RecipeJoinMember, ...], MinLen(min_length=2)], operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_registration_id: str, intent: dict[str, JsonValue] = <factory>, target_options: dict[str, JsonValue] = <factory>, projections: tuple[stove0_recipe_config.models.OperationProjection, ...] = (), input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only') -> None\""
    },
    "RecipeJoinMember": {
      "kind": "class",
      "members": {
        "canonical_roles": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "f96f0cc79aff67369a99fa4bb8d4b8f29e89bdd258e990af58d3e937574ad43f",
      "signature": "\"(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], output_roles: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)]) -> None\""
    },
    "RecipeRoute": {
      "kind": "class",
      "schema_sha256": "c177dec4c92471097826d683c4b3dae92c62fdcc31cfcd5a840db283249541b4",
      "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], when: tuple[stove0_recipe_config.models.FactPredicate, ...] = (), artifact_rules: tuple[stove0_recipe_config.models.ArtifactRule, ...] = (ArtifactRule(glob='*', role='stove0.source/v1', media_type=None),), primary_role: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)]] = None, associated_roles: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...] = (), intent: dict[str, JsonValue] = <factory>, projections: tuple[stove0_recipe_config.models.OperationProjection, ...] = (), kind: Literal['operation'] = 'operation', operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_registration_id: str, target_options: dict[str, JsonValue] = <factory>, input_retrieval_policy: Literal['available-only', 'allow'] = 'available-only') -> None\""
    }
  },
  "module": "stove0_recipe_config"
}
```
