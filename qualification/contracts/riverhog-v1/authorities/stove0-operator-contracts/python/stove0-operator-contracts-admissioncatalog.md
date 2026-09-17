# stove0_operator_contracts.AdmissionCatalog

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissioncatalog:95e5135c66 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6dcc5be0e9"></a>
- <a id="s-3ab6af11dd"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-d3474dcc8d"></a>`module`: `stove0_operator_contracts`
- <a id="s-dc103249f8"></a>`name`: `AdmissionCatalog`
- <a id="s-b029c2dcf2"></a>`unit`: `export`

### Declared structure

- <a id="s-4e0c80bc17"></a>`kind`: `"class"`
- <a id="s-6526b9a9fa"></a>`signature`: `"\"(*, format: Literal['stove0-admissions/v1'] = 'stove0-admissions/v1', policies: Annotated[tuple[stove0_operator_contracts.AdmissionPolicy, ...], MaxLen(max_length=100)] = ()) -> None\""`

#### Validated model schema

<a id="s-e70a4b1460"></a>

- <a id="s-42d5ab1a2b"></a>`type`: `"object"`
- <a id="s-12d143e7aa"></a>`additionalProperties`: `false`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-844450f375"></a>`format` | no | type="string"; const="stove0-admissions/v1"; default="stove0-admissions/v1" |  |
| <a id="s-4996621053"></a>`policies` | no | type="array"; default=[]; items=([AdmissionPolicy](#s-0d836ab3d0)); maxItems=100; x-riverhog-extent={"policy":"contract_max","reason":"bounded-deployment-admission-catalog"} |  |

##### Definitions

- [AdmissionPolicy](#s-0d836ab3d0)
- [CollectionTag](#s-faf360b591)
- [JsonValue](#s-0f94759ee8)

##### <a id="s-0d836ab3d0"></a>definition `AdmissionPolicy`

- <a id="s-ffcf16cbe8"></a>`type`: `"object"`
- <a id="s-76bb036182"></a>`additionalProperties`: `false`
- <a id="s-c9e53d2402"></a>`required`: `["id","revision","required_tags","recipe_id","recipe_revision","recipe_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-50b9ab1501"></a>`automatic_preview` | no | type="string"; const="accept-ready"; default="accept-ready" |  |
| <a id="s-f479d92581"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-0f94759ee8)) |  |
| <a id="s-770ef3f34e"></a>`format` | no | type="string"; const="stove0-admission-policy/v1"; default="stove0-admission-policy/v1" |  |
| <a id="s-941227cf31"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-802dadf68b"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-200d5fef77"></a>`recipe_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-a9687c9bfe"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e8dc91bade"></a>`required_tags` | yes | type="array"; items=([CollectionTag](#s-faf360b591)); maxItems=100; minItems=1; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |
| <a id="s-f83795db7e"></a>`revision` | yes | type="integer"; minimum=1 |  |

##### <a id="s-faf360b591"></a>definition `CollectionTag`

- <a id="s-3ce0f0179e"></a>`type`: `"string"`
- <a id="s-d680904f0f"></a>`maxLength`: `65536`
- <a id="s-c6d8b78acf"></a>`minLength`: `1`
- <a id="s-daf761a541"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-1108b0f664"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-151b35fc57"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-0f94759ee8"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [canonical_policies](stove0-operator-contracts-admissioncatalog-canonical-policies.md)
- [catalog_sha256](stove0-operator-contracts-admissioncatalog-catalog-sha256.md)

## Governing policies

- <a id="pa-ece43c0ae8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionCatalog`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 401c080d68cad2f5e4adddcbf4198e2a14bb2ee9153dcc5bc587b1f7876ded0b -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "AdmissionPolicy": {
          "additionalProperties": false,
          "properties": {
            "automatic_preview": {
              "const": "accept-ready",
              "default": "accept-ready",
              "type": "string"
            },
            "effective_intent": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "format": {
              "const": "stove0-admission-policy/v1",
              "default": "stove0-admission-policy/v1",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "recipe_id": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
            "recipe_revision": {
              "minimum": 1,
              "type": "integer"
            },
            "recipe_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "required_tags": {
              "items": {
                "$ref": "#/$defs/CollectionTag"
              },
              "maxItems": 100,
              "minItems": 1,
              "type": "array",
              "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-exact-classification-admission-predicate"
              }
            },
            "revision": {
              "minimum": 1,
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
          "type": "object"
        },
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
      "properties": {
        "format": {
          "const": "stove0-admissions/v1",
          "default": "stove0-admissions/v1",
          "type": "string"
        },
        "policies": {
          "default": [],
          "items": {
            "$ref": "#/$defs/AdmissionPolicy"
          },
          "maxItems": 100,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-deployment-admission-catalog"
          }
        }
      },
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-admissions/v1'] = 'stove0-admissions/v1', policies: Annotated[tuple[stove0_operator_contracts.AdmissionPolicy, ...], MaxLen(max_length=100)] = ()) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionCatalog",
  "unit": "export"
}
```

</details>
