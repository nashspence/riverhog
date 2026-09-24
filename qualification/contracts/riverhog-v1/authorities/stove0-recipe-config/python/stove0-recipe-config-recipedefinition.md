# stove0_recipe_config.RecipeDefinition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-recipe-config:stove0-recipe-config-recipedefinition:c7c878259b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-recipe-config](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0119d3013a"></a>
- <a id="s-e4c3252c02"></a>`distribution`: `stove0-recipe-config`
- <a id="s-20906aff87"></a>`module`: `stove0_recipe_config`
- <a id="s-805f9f2ed8"></a>`name`: `RecipeDefinition`
- <a id="s-41370f7180"></a>`unit`: `export`

### Declared structure

- <a id="s-be43a28ae7"></a>`kind`: `"class"`
- <a id="s-90380c0a70"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], revision: Annotated[NonnegativeDecimal, Ge(ge=1)], event_input_closure: Literal['single-finalized-collection'] = 'single-finalized-collection', artifact_associations: tuple[stove0_recipe_config.models.ArtifactAssociation, ...] = (), observers: tuple[stove0_recipe_config.models.ObserverUse, ...] = (), routes: Annotated[tuple[Annotated[stove0_recipe_config.models.RecipeRoute \| stove0_recipe_config.models.RecipeCoordinationRoute, FieldInfo(annotation=NoneType, required=True, discriminator='kind')], ...], MinLen(min_length=1)], unmatched_artifact_disposition: Literal['retain-in-source', 'reject-work'], allow_derived_inputs: bool = False, source_collection_retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', source_collection_retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, join: stove0_recipe_config.models.RecipeJoin \| None = None) -> None\""`

#### Validated model schema

<a id="s-28244ebf9c"></a>

- <a id="s-96c4c66c30"></a>`type`: `"object"`
- <a id="s-f6f7d4fd73"></a>`additionalProperties`: `false`
- <a id="s-3e2106fafc"></a>`required`: `["id","revision","routes","unmatched_artifact_disposition"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-11491989e3"></a>`allow_derived_inputs` | no | type="boolean"; default=false |  |
| <a id="s-e35545034f"></a>`artifact_associations` | no | type="array"; default=[]; items=([ArtifactAssociation](#s-cbe0c73c25)) |  |
| <a id="s-130d6caa97"></a>`event_input_closure` | no | type="string"; const="single-finalized-collection"; default="single-finalized-collection" |  |
| <a id="s-080eb4720c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-006a434cc1"></a>`join` | no | anyOf=[([RecipeJoin](#s-109200a17a)); (type="null")]; default=null |  |
| <a id="s-fd94ee5952"></a>`observers` | no | type="array"; default=[]; items=([ObserverUse](#s-f32a9323a1)) |  |
| <a id="s-d6c2d22f1a"></a>`revision` | yes | [NonnegativeDecimal](#s-7e24659325); ge=1 |  |
| <a id="s-a6aff0f971"></a>`routes` | yes | type="array"; items=(discriminator={"mapping":{"coordination":"#/$defs/RecipeCoordinationRoute","operation":"#/$defs/RecipeRoute"},"propertyName":"kind"}; oneOf=[([RecipeRoute](#s-748c3d5210)); ([RecipeCoordinationRoute](#s-13711c075a))]); minItems=1 |  |
| <a id="s-c022e6bcf8"></a>`source_collection_retirement_grace_seconds` | no | type="integer"; minimum=0; default=0 |  |
| <a id="s-0f03e1b1bf"></a>`source_collection_retirement_policy` | no | type="string"; enum=["retain","retire-after-verified-output"]; default="retain" |  |
| <a id="s-2712f1d519"></a>`unmatched_artifact_disposition` | yes | type="string"; enum=["retain-in-source","reject-work"] |  |

##### Definitions

- [ArtifactAssociation](#s-cbe0c73c25)
- [ArtifactFactBinding](#s-3a0df3e117)
- [ArtifactRule](#s-043cb19d8b)
- [FactPredicate](#s-555441cc57)
- [JsonValue](#s-547817020e)
- [NonnegativeDecimal](#s-7e24659325)
- [ObserverUse](#s-f32a9323a1)
- [OperationProjection](#s-d265eb178c)
- [RecipeCoordinationRoute](#s-13711c075a)
- [RecipeIdentityRef](#s-3629b2ee5e)
- [RecipeJoin](#s-109200a17a)
- [RecipeJoinMember](#s-e7fea4754a)
- [RecipeRoute](#s-748c3d5210)

##### <a id="s-cbe0c73c25"></a>definition `ArtifactAssociation`

- <a id="s-de5fcbff74"></a>`type`: `"object"`
- <a id="s-862d07ad3d"></a>`additionalProperties`: `false`
- <a id="s-380dfe3755"></a>`required`: `["primary_role","associated_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1f8709f4e0"></a>`associated_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |
| <a id="s-38c97a2e5b"></a>`path_identity` | no | type="string"; const="same-parent-stem"; default="same-parent-stem" |  |
| <a id="s-e3ccf2ad58"></a>`primary_role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-3a0df3e117"></a>definition `ArtifactFactBinding`

- <a id="s-b12f4ab68d"></a>`type`: `"object"`
- <a id="s-fd6f455f54"></a>`additionalProperties`: `false`
- <a id="s-4b4304a7d7"></a>`required`: `["records_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-83d649e68d"></a>`artifact_id_pointer` | no | type="string"; default="/artifact_id"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-7135ac6fed"></a>`records_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-043cb19d8b"></a>definition `ArtifactRule`

- <a id="s-c6a66ab268"></a>`type`: `"object"`
- <a id="s-67e3386691"></a>`additionalProperties`: `false`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a2b3824d26"></a>`glob` | no | type="string"; default="*" |  |
| <a id="s-1ad60d1712"></a>`media_type` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-50f1151baf"></a>`role` | no | type="string"; default="stove0.source/v1"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |

##### <a id="s-555441cc57"></a>definition `FactPredicate`

- <a id="s-ef1f7abd7e"></a>`type`: `"object"`
- <a id="s-cf186d4317"></a>`additionalProperties`: `false`
- <a id="s-a00b4e9357"></a>`required`: `["observation_contract_id","pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4d80a42436"></a>`artifact_facts` | no | anyOf=[([ArtifactFactBinding](#s-3a0df3e117)); (type="null")]; default=null |  |
| <a id="s-610771be53"></a>`artifact_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-58ad48b117"></a>`observation_contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2ae98b5b08"></a>`operator` | no | type="string"; enum=["equals","not-equals","contains","exists"]; default="equals" |  |
| <a id="s-4fb4531dbc"></a>`pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-560913d937"></a>`value` | no | [JsonValue](#s-547817020e); default=null |  |

##### <a id="s-547817020e"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-7e24659325"></a>definition `NonnegativeDecimal`

- <a id="s-e5c334d3e9"></a>`type`: `"string"`
- <a id="s-93a9cd9349"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

##### <a id="s-f32a9323a1"></a>definition `ObserverUse`

- <a id="s-559472991b"></a>`type`: `"object"`
- <a id="s-168c302f4f"></a>`additionalProperties`: `false`
- <a id="s-2d6c927158"></a>`required`: `["registration_id","contract_id","contract_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ba118fc5d2"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-043cb19d8b)) |  |
| <a id="s-ad5345da63"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-05e39ea572"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-4893873f0b"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864; default=1048576 |  |
| <a id="s-5563a0147b"></a>`options` | no | type="object"; additionalProperties=([JsonValue](#s-547817020e)) |  |
| <a id="s-43625ada0c"></a>`registration_id` | yes | type="string" |  |
| <a id="s-9c83a96cba"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-1fe1e6bffc"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400; default=300 |  |

##### <a id="s-d265eb178c"></a>definition `OperationProjection`

- <a id="s-c6f6ba5586"></a>`type`: `"object"`
- <a id="s-caf102c6ed"></a>`additionalProperties`: `false`
- <a id="s-38cbe9a260"></a>`required`: `["source","source_pointer","destination","destination_pointer"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cc69e15b87"></a>`destination` | yes | type="string"; enum=["intent","target-options"] |  |
| <a id="s-6a6ec63046"></a>`destination_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |
| <a id="s-b7af491461"></a>`source` | yes | type="string"; enum=["work-effective-intent","work-evaluation"] |  |
| <a id="s-6f562346cf"></a>`source_pointer` | yes | type="string"; pattern="^(?:\|/(?:[^~/]\|~[01])*(?:/(?:[^~/]\|~[01])*)*)$" |  |

##### <a id="s-13711c075a"></a>definition `RecipeCoordinationRoute`

- <a id="s-8446b2c7d4"></a>`type`: `"object"`
- <a id="s-ce39d5ad14"></a>`additionalProperties`: `false`
- <a id="s-6915dcb666"></a>`required`: `["id","recipe"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-805f094002"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-043cb19d8b)) |  |
| <a id="s-3894864f00"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-be35dc147b"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-579f8e12e9"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-547817020e)) |  |
| <a id="s-af7539a7c6"></a>`kind` | no | type="string"; const="coordination"; default="coordination" |  |
| <a id="s-8da450bc82"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null |  |
| <a id="s-7915ccbd54"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-d265eb178c)) |  |
| <a id="s-c9fda21afe"></a>`recipe` | yes | [RecipeIdentityRef](#s-3629b2ee5e) |  |
| <a id="s-47b6c79255"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-555441cc57)) |  |

##### <a id="s-3629b2ee5e"></a>definition `RecipeIdentityRef`

- <a id="s-2e68f3df78"></a>`type`: `"object"`
- <a id="s-04193ca763"></a>`additionalProperties`: `false`
- <a id="s-decf7dfcc4"></a>`required`: `["id","revision","sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1fdd519ec2"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b72d19b8a9"></a>`revision` | yes | [NonnegativeDecimal](#s-7e24659325); ge=1 |  |
| <a id="s-b648d34c36"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-109200a17a"></a>definition `RecipeJoin`

- <a id="s-b2c79e736e"></a>`type`: `"object"`
- <a id="s-1700fccfd6"></a>`additionalProperties`: `false`
- <a id="s-36e829d873"></a>`required`: `["id","members","operation_id","target_registration_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4a69b671cb"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-64ea4ddb84"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-66c1eda131"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-547817020e)) |  |
| <a id="s-5450af07d4"></a>`members` | yes | type="array"; items=([RecipeJoinMember](#s-e7fea4754a)); minItems=2 |  |
| <a id="s-941a2ae588"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2ed70da359"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-d265eb178c)) |  |
| <a id="s-c7508a0048"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-547817020e)) |  |
| <a id="s-aa6d1aab25"></a>`target_registration_id` | yes | type="string" |  |

##### <a id="s-e7fea4754a"></a>definition `RecipeJoinMember`

- <a id="s-df1d3bb220"></a>`type`: `"object"`
- <a id="s-9265bd7e61"></a>`additionalProperties`: `false`
- <a id="s-3aa437343e"></a>`required`: `["branch_id","output_roles"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0fd4aa9ed3"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-c0e09bf207"></a>`output_roles` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

##### <a id="s-748c3d5210"></a>definition `RecipeRoute`

- <a id="s-77e774fffe"></a>`type`: `"object"`
- <a id="s-5c73467459"></a>`additionalProperties`: `false`
- <a id="s-81d02b0ea6"></a>`required`: `["id","operation_id","target_registration_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-470099dd76"></a>`artifact_rules` | no | type="array"; default=[{"glob":"*","media_type":null,"role":"stove0.source/v1"}]; items=([ArtifactRule](#s-043cb19d8b)) |  |
| <a id="s-0913447875"></a>`associated_roles` | no | type="array"; default=[]; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$") |  |
| <a id="s-7581eaa701"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e73c5bc771"></a>`input_retrieval_policy` | no | type="string"; enum=["available-only","allow"]; default="available-only" |  |
| <a id="s-2c5fcb5798"></a>`intent` | no | type="object"; additionalProperties=([JsonValue](#s-547817020e)) |  |
| <a id="s-3c4716ec96"></a>`kind` | no | type="string"; const="operation"; default="operation" |  |
| <a id="s-a2a262622f"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4049b4c976"></a>`primary_role` | no | anyOf=[(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); (type="null")]; default=null |  |
| <a id="s-5acb8cc927"></a>`projections` | no | type="array"; default=[]; items=([OperationProjection](#s-d265eb178c)) |  |
| <a id="s-13ea0c3b0e"></a>`target_options` | no | type="object"; additionalProperties=([JsonValue](#s-547817020e)) |  |
| <a id="s-274a4f47d3"></a>`target_registration_id` | yes | type="string" |  |
| <a id="s-12af4e291d"></a>`when` | no | type="array"; default=[]; items=([FactPredicate](#s-555441cc57)) |  |

## Maintained corroboration

### Related interface records

- [canonical_members](stove0-recipe-config-recipedefinition-canonical-members.md)
- [identity_document](stove0-recipe-config-recipedefinition-identity-document.md)
- [ref](stove0-recipe-config-recipedefinition-ref.md)
- [sha256](stove0-recipe-config-recipedefinition-sha256.md)

## Governing policies

- <a id="pa-f994c22696"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-recipe-config:stove0_recipe_config](../../../evidence/sources/authorities.md#src-9e1422d2d6) — [some-implementations/stove0/packages/recipe-config/src/stove0\_recipe\_config/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/recipe-config/src/stove0_recipe_config/__init__.py)

### Machine authority

- `/external_contract/python/stove0_recipe_config.RecipeDefinition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 978cd871ccd751a4666acebf78ad14323554b0673a36bbbbc9d772f722f37ddf -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactAssociation": {
          "additionalProperties": false,
          "properties": {
            "associated_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            },
            "path_identity": {
              "const": "same-parent-stem",
              "default": "same-parent-stem",
              "type": "string"
            },
            "primary_role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "required": [
            "primary_role",
            "associated_roles"
          ],
          "type": "object"
        },
        "ArtifactFactBinding": {
          "additionalProperties": false,
          "properties": {
            "artifact_id_pointer": {
              "default": "/artifact_id",
              "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
              "type": "string"
            },
            "records_pointer": {
              "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
              "type": "string"
            }
          },
          "required": [
            "records_pointer"
          ],
          "type": "object"
        },
        "ArtifactRule": {
          "additionalProperties": false,
          "properties": {
            "glob": {
              "default": "*",
              "type": "string"
            },
            "media_type": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "role": {
              "default": "stove0.source/v1",
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            }
          },
          "type": "object"
        },
        "FactPredicate": {
          "additionalProperties": false,
          "properties": {
            "artifact_facts": {
              "anyOf": [
                {
                  "$ref": "#/$defs/ArtifactFactBinding"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "artifact_roles": {
              "default": [],
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "type": "array"
            },
            "observation_contract_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "operator": {
              "default": "equals",
              "enum": [
                "equals",
                "not-equals",
                "contains",
                "exists"
              ],
              "type": "string"
            },
            "pointer": {
              "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
              "type": "string"
            },
            "value": {
              "$ref": "#/$defs/JsonValue",
              "default": null
            }
          },
          "required": [
            "observation_contract_id",
            "pointer"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        },
        "ObserverUse": {
          "additionalProperties": false,
          "properties": {
            "artifact_rules": {
              "default": [
                {
                  "glob": "*",
                  "media_type": null,
                  "role": "stove0.source/v1"
                }
              ],
              "items": {
                "$ref": "#/$defs/ArtifactRule"
              },
              "type": "array"
            },
            "contract_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "maximum_result_bytes": {
              "default": 1048576,
              "maximum": 67108864,
              "minimum": 1,
              "type": "integer"
            },
            "options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "registration_id": {
              "type": "string"
            },
            "retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "type": "string"
            },
            "timeout_seconds": {
              "default": 300,
              "maximum": 86400,
              "minimum": 1,
              "type": "integer"
            }
          },
          "required": [
            "registration_id",
            "contract_id",
            "contract_sha256"
          ],
          "type": "object"
        },
        "OperationProjection": {
          "additionalProperties": false,
          "properties": {
            "destination": {
              "enum": [
                "intent",
                "target-options"
              ],
              "type": "string"
            },
            "destination_pointer": {
              "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
              "type": "string"
            },
            "source": {
              "enum": [
                "work-effective-intent",
                "work-evaluation"
              ],
              "type": "string"
            },
            "source_pointer": {
              "pattern": "^(?:|/(?:[^~/]|~[01])*(?:/(?:[^~/]|~[01])*)*)$",
              "type": "string"
            }
          },
          "required": [
            "source",
            "source_pointer",
            "destination",
            "destination_pointer"
          ],
          "type": "object"
        },
        "RecipeCoordinationRoute": {
          "additionalProperties": false,
          "properties": {
            "artifact_rules": {
              "default": [
                {
                  "glob": "*",
                  "media_type": null,
                  "role": "stove0.source/v1"
                }
              ],
              "items": {
                "$ref": "#/$defs/ArtifactRule"
              },
              "type": "array"
            },
            "associated_roles": {
              "default": [],
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "type": "array"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "kind": {
              "const": "coordination",
              "default": "coordination",
              "type": "string"
            },
            "primary_role": {
              "anyOf": [
                {
                  "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "projections": {
              "default": [],
              "items": {
                "$ref": "#/$defs/OperationProjection"
              },
              "type": "array"
            },
            "recipe": {
              "$ref": "#/$defs/RecipeIdentityRef"
            },
            "when": {
              "default": [],
              "items": {
                "$ref": "#/$defs/FactPredicate"
              },
              "type": "array"
            }
          },
          "required": [
            "id",
            "recipe"
          ],
          "type": "object"
        },
        "RecipeIdentityRef": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "revision": {
              "$ref": "#/$defs/NonnegativeDecimal",
              "ge": 1
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "revision",
            "sha256"
          ],
          "type": "object"
        },
        "RecipeJoin": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "input_retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "type": "string"
            },
            "intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "members": {
              "items": {
                "$ref": "#/$defs/RecipeJoinMember"
              },
              "minItems": 2,
              "type": "array"
            },
            "operation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "projections": {
              "default": [],
              "items": {
                "$ref": "#/$defs/OperationProjection"
              },
              "type": "array"
            },
            "target_options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "target_registration_id": {
              "type": "string"
            }
          },
          "required": [
            "id",
            "members",
            "operation_id",
            "target_registration_id"
          ],
          "type": "object"
        },
        "RecipeJoinMember": {
          "additionalProperties": false,
          "properties": {
            "branch_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "output_roles": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "branch_id",
            "output_roles"
          ],
          "type": "object"
        },
        "RecipeRoute": {
          "additionalProperties": false,
          "properties": {
            "artifact_rules": {
              "default": [
                {
                  "glob": "*",
                  "media_type": null,
                  "role": "stove0.source/v1"
                }
              ],
              "items": {
                "$ref": "#/$defs/ArtifactRule"
              },
              "type": "array"
            },
            "associated_roles": {
              "default": [],
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "type": "array"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "input_retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "type": "string"
            },
            "intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "kind": {
              "const": "operation",
              "default": "operation",
              "type": "string"
            },
            "operation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "primary_role": {
              "anyOf": [
                {
                  "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "projections": {
              "default": [],
              "items": {
                "$ref": "#/$defs/OperationProjection"
              },
              "type": "array"
            },
            "target_options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "target_registration_id": {
              "type": "string"
            },
            "when": {
              "default": [],
              "items": {
                "$ref": "#/$defs/FactPredicate"
              },
              "type": "array"
            }
          },
          "required": [
            "id",
            "operation_id",
            "target_registration_id"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "allow_derived_inputs": {
          "default": false,
          "type": "boolean"
        },
        "artifact_associations": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ArtifactAssociation"
          },
          "type": "array"
        },
        "event_input_closure": {
          "const": "single-finalized-collection",
          "default": "single-finalized-collection",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "join": {
          "anyOf": [
            {
              "$ref": "#/$defs/RecipeJoin"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "observers": {
          "default": [],
          "items": {
            "$ref": "#/$defs/ObserverUse"
          },
          "type": "array"
        },
        "revision": {
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
        },
        "routes": {
          "items": {
            "discriminator": {
              "mapping": {
                "coordination": "#/$defs/RecipeCoordinationRoute",
                "operation": "#/$defs/RecipeRoute"
              },
              "propertyName": "kind"
            },
            "oneOf": [
              {
                "$ref": "#/$defs/RecipeRoute"
              },
              {
                "$ref": "#/$defs/RecipeCoordinationRoute"
              }
            ]
          },
          "minItems": 1,
          "type": "array"
        },
        "source_collection_retirement_grace_seconds": {
          "default": 0,
          "minimum": 0,
          "type": "integer"
        },
        "source_collection_retirement_policy": {
          "default": "retain",
          "enum": [
            "retain",
            "retire-after-verified-output"
          ],
          "type": "string"
        },
        "unmatched_artifact_disposition": {
          "enum": [
            "retain-in-source",
            "reject-work"
          ],
          "type": "string"
        }
      },
      "required": [
        "id",
        "revision",
        "routes",
        "unmatched_artifact_disposition"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], revision: Annotated[NonnegativeDecimal, Ge(ge=1)], event_input_closure: Literal['single-finalized-collection'] = 'single-finalized-collection', artifact_associations: tuple[stove0_recipe_config.models.ArtifactAssociation, ...] = (), observers: tuple[stove0_recipe_config.models.ObserverUse, ...] = (), routes: Annotated[tuple[Annotated[stove0_recipe_config.models.RecipeRoute | stove0_recipe_config.models.RecipeCoordinationRoute, FieldInfo(annotation=NoneType, required=True, discriminator='kind')], ...], MinLen(min_length=1)], unmatched_artifact_disposition: Literal['retain-in-source', 'reject-work'], allow_derived_inputs: bool = False, source_collection_retirement_policy: Literal['retain', 'retire-after-verified-output'] = 'retain', source_collection_retirement_grace_seconds: Annotated[int, Ge(ge=0)] = 0, join: stove0_recipe_config.models.RecipeJoin | None = None) -> None\""
  },
  "distribution": "stove0-recipe-config",
  "module": "stove0_recipe_config",
  "name": "RecipeDefinition",
  "unit": "export"
}
```

</details>
