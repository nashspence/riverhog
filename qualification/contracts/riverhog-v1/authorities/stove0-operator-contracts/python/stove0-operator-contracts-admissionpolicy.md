# stove0_operator_contracts.AdmissionPolicy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionpolicy:37fbbc74d4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b469ad5ed5"></a>
- <a id="s-6c74bb7c21"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-e11f6dbc15"></a>`module`: `stove0_operator_contracts`
- <a id="s-85d262fff3"></a>`name`: `AdmissionPolicy`
- <a id="s-027fdcf56d"></a>`unit`: `export`

### Declared structure

- <a id="s-91266566bb"></a>`kind`: `"class"`
- <a id="s-6c8e30fa14"></a>`signature`: `"\"(*, format: Literal['stove0-admission-policy/v1'] = 'stove0-admission-policy/v1', id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$')], revision: Annotated[int, Ge(ge=1)], required_tags: Annotated[tuple[CollectionTag, ...], MinLen(min_length=1), MaxLen(max_length=100)], recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int, Ge(ge=1)], recipe_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], effective_intent: dict[str, JsonValue] = <factory>, automatic_preview: Literal['accept-ready'] = 'accept-ready') -> None\""`

#### Validated model schema

<a id="s-886f287521"></a>
- <a id="s-9505a2f04d"></a>`title`: AdmissionPolicy
- <a id="s-a086692f6e"></a>`description`: One bounded, exact all-of classification admission rule.
- <a id="s-845b3374ea"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-eaea3852d7"></a>`automatic_preview` | no | type="string"; const="accept-ready" |  |
| <a id="s-443f9b71cf"></a>`effective_intent` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-9bad2e23cc"></a>`format` | no | type="string"; const="stove0-admission-policy/v1" |  |
| <a id="s-c56cfe0f78"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-4ea2bba8aa"></a>`recipe_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-e69fcbefed"></a>`recipe_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-69008c4330"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ae61a1b632"></a>`required_tags` | yes | type="array"; minItems=1; maxItems=100; items=(#/$defs/CollectionTag); additional keys=`x-riverhog-extent` |  |
| <a id="s-7e13c20b0c"></a>`revision` | yes | type="integer"; minimum=1 |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-e223a27a21"></a>`CollectionTag` | type="string"; minLength=1; maxLength=65536; additional keys=`x-riverhog-encoded-bytes-max`, `x-riverhog-extent`, `x-unicode-normalization` |
| <a id="s-7830c390f5"></a>`JsonValue` | empty object |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.AdmissionPolicy.canonical_required_tags](stove0-operator-contracts-admissionpolicy-canonical-required-tags.md)
- [stove0_operator_contracts.AdmissionPolicy.policy_sha256](stove0-operator-contracts-admissionpolicy-policy-sha256.md)

## Governing policies

- <a id="pa-ce091d4582"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPolicy`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 720921e353ccb16f9e94fe0712e6c3e4be008b189227bc9c762042560d3274fa -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionTag": {
          "maxLength": 65536,
          "minLength": 1,
          "type": "string",
          "x-riverhog-encoded-bytes-max": 65536,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-human-authored-collection-tag"
          },
          "x-unicode-normalization": "NFC"
        },
        "JsonValue": {}
      },
      "additionalProperties": false,
      "description": "One bounded, exact all-of classification admission rule.",
      "properties": {
        "automatic_preview": {
          "const": "accept-ready",
          "default": "accept-ready",
          "title": "Automatic Preview",
          "type": "string"
        },
        "effective_intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Effective Intent",
          "type": "object"
        },
        "format": {
          "const": "stove0-admission-policy/v1",
          "default": "stove0-admission-policy/v1",
          "title": "Format",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "recipe_id": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Recipe Id",
          "type": "string"
        },
        "recipe_revision": {
          "minimum": 1,
          "title": "Recipe Revision",
          "type": "integer"
        },
        "recipe_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Recipe Sha256",
          "type": "string"
        },
        "required_tags": {
          "items": {
            "$ref": "#/$defs/CollectionTag"
          },
          "maxItems": 100,
          "minItems": 1,
          "title": "Required Tags",
          "type": "array",
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-exact-classification-admission-predicate"
          }
        },
        "revision": {
          "minimum": 1,
          "title": "Revision",
          "type": "integer"
        }
      },
      "required": [
        "id",
        "revision",
        "required_tags",
        "recipe_id",
        "recipe_revision",
        "recipe_sha256"
      ],
      "title": "AdmissionPolicy",
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-admission-policy/v1'] = 'stove0-admission-policy/v1', id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$')], revision: Annotated[int, Ge(ge=1)], required_tags: Annotated[tuple[CollectionTag, ...], MinLen(min_length=1), MaxLen(max_length=100)], recipe_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], recipe_revision: Annotated[int, Ge(ge=1)], recipe_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], effective_intent: dict[str, JsonValue] = <factory>, automatic_preview: Literal['accept-ready'] = 'accept-ready') -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionPolicy",
  "unit": "export"
}
```
