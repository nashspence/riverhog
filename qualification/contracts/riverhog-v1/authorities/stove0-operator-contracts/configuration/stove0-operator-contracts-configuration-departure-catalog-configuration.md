# stove0-operator-contracts:configuration:departure-catalog configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-operator-contracts:stove0-operator-contracts-configuration-d-e398f0eb9d:7b02f7b9f8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-f39cea53a2"></a>

- <a id="s-10855c707c"></a>`type`: `"object"`
- <a id="s-69218de27b"></a>`additionalProperties`: `false`
- <a id="s-dc712a75d7"></a>`title`: `"DepartureCatalog"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1294687c5b"></a>`format` | no | type="string"; const="stove0-departures/v1"; default="stove0-departures/v1"; title="Format" |  |
| <a id="s-61dc2ce8be"></a>`policies` | no | type="array"; default=[]; items=([DeparturePolicy](#s-3238adf8d6)); maxItems=100; title="Policies"; x-riverhog-extent={"policy":"contract_max","reason":"bounded-deployment-departure-catalog"} |  |

### Definitions

- [AllVisibleAdmissionSelector](#s-ba4a4f1975)
- [CollectionTag](#s-a922db8b95)
- [DeparturePolicy](#s-3238adf8d6)
- [TaggedAdmissionSelector](#s-9d5163772f)

### <a id="s-ba4a4f1975"></a>definition `AllVisibleAdmissionSelector`

- <a id="s-eb7aedc68f"></a>`type`: `"object"`
- <a id="s-c05bb9a45b"></a>`additionalProperties`: `false`
- <a id="s-1995dd7a96"></a>`title`: `"AllVisibleAdmissionSelector"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5c6d91b5ee"></a>`kind` | no | type="string"; const="all"; default="all"; title="Kind" |  |

### <a id="s-a922db8b95"></a>definition `CollectionTag`

- <a id="s-39ff68ac86"></a>`type`: `"string"`
- <a id="s-7bcf69f9c1"></a>`maxLength`: `65536`
- <a id="s-7ccd32906f"></a>`minLength`: `1`
- <a id="s-c4c3c3cbb5"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-346cb32aba"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-0582d6d3ff"></a>`x-unicode-normalization`: `"NFC"`

### <a id="s-3238adf8d6"></a>definition `DeparturePolicy`

- <a id="s-4e8256dad5"></a>`type`: `"object"`
- <a id="s-25b24ee061"></a>`additionalProperties`: `false`
- <a id="s-4d421cf646"></a>`description`: `"A catalog departure subscription with no recipe or artifact authority."`
- <a id="s-c5f2ff96d4"></a>`required`: `["id","revision","selector","target_registration_id","target_identity"]`
- <a id="s-7b844bad35"></a>`title`: `"DeparturePolicy"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-83b1958fea"></a>`format` | no | type="string"; const="stove0-departure-policy/v1"; default="stove0-departure-policy/v1"; title="Format" |  |
| <a id="s-4332f2266f"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-7581936896"></a>`revision` | yes | type="integer"; minimum=1; title="Revision" |  |
| <a id="s-c96ffd847e"></a>`selector` | yes | discriminator={"mapping":{"all":"#/$defs/AllVisibleAdmissionSelector","tags":"#/$defs/TaggedAdmissionSelector"},"propertyName":"kind"}; oneOf=[([AllVisibleAdmissionSelector](#s-ba4a4f1975)); ([TaggedAdmissionSelector](#s-9d5163772f))]; title="Selector" |  |
| <a id="s-3b307038d8"></a>`target_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Target Identity" |  |
| <a id="s-d999dbe52d"></a>`target_registration_id` | yes | type="string"; maxLength=160; minLength=1; title="Target Registration Id" |  |

### <a id="s-9d5163772f"></a>definition `TaggedAdmissionSelector`

- <a id="s-4cd5e265f9"></a>`type`: `"object"`
- <a id="s-4c8f6ba959"></a>`additionalProperties`: `false`
- <a id="s-83c45a1394"></a>`required`: `["required"]`
- <a id="s-2fd0f60af3"></a>`title`: `"TaggedAdmissionSelector"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e8eb7c2b20"></a>`kind` | no | type="string"; const="tags"; default="tags"; title="Kind" |  |
| <a id="s-29bc0ca9e5"></a>`required` | yes | type="array"; items=([CollectionTag](#s-a922db8b95)); maxItems=100; minItems=1; title="Required"; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition CollectionTag](#s-a922db8b95) | `encoded-size · bytes · contract_max` | maximum=65536; reason="bounded-human-authored-collection-tag"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [definition CollectionTag](#s-a922db8b95) | `length · characters · contract_max` | maximum=65536; minimum=1; reason="schema-maximum" |
| [definition DeparturePolicy · field target_identity](#s-3b307038d8) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition DeparturePolicy · field target_registration_id](#s-d999dbe52d) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [definition TaggedAdmissionSelector · field required](#s-29bc0ca9e5) | `cardinality · items · contract_max` | maximum=100; minimum=1; reason="bounded-exact-classification-admission-predicate" |
| [field policies](#s-61dc2ce8be) | `cardinality · items · contract_max` | maximum=100; reason="bounded-deployment-departure-catalog" |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-6919323f0a"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-e39530102d"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:stove0-operator-contracts:configuration:departure-catalog](../../../evidence/sources/authorities.md#src-ca1752be86) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py::DepartureCatalog](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/stove0-operator-contracts:configuration:departure-catalog`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2696da84141f69b6a7b6db9b00afa626f3832d9f57ec28a89c1961b2e63c0a97 -->

```json
{
  "$defs": {
    "AllVisibleAdmissionSelector": {
      "additionalProperties": false,
      "properties": {
        "kind": {
          "const": "all",
          "default": "all",
          "title": "Kind",
          "type": "string"
        }
      },
      "title": "AllVisibleAdmissionSelector",
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
    "DeparturePolicy": {
      "additionalProperties": false,
      "description": "A catalog departure subscription with no recipe or artifact authority.",
      "properties": {
        "format": {
          "const": "stove0-departure-policy/v1",
          "default": "stove0-departure-policy/v1",
          "title": "Format",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "revision": {
          "minimum": 1,
          "title": "Revision",
          "type": "integer"
        },
        "selector": {
          "discriminator": {
            "mapping": {
              "all": "#/$defs/AllVisibleAdmissionSelector",
              "tags": "#/$defs/TaggedAdmissionSelector"
            },
            "propertyName": "kind"
          },
          "oneOf": [
            {
              "$ref": "#/$defs/AllVisibleAdmissionSelector"
            },
            {
              "$ref": "#/$defs/TaggedAdmissionSelector"
            }
          ],
          "title": "Selector"
        },
        "target_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Target Identity",
          "type": "string"
        },
        "target_registration_id": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Target Registration Id",
          "type": "string"
        }
      },
      "required": [
        "id",
        "revision",
        "selector",
        "target_registration_id",
        "target_identity"
      ],
      "title": "DeparturePolicy",
      "type": "object"
    },
    "TaggedAdmissionSelector": {
      "additionalProperties": false,
      "properties": {
        "kind": {
          "const": "tags",
          "default": "tags",
          "title": "Kind",
          "type": "string"
        },
        "required": {
          "items": {
            "$ref": "#/$defs/CollectionTag"
          },
          "maxItems": 100,
          "minItems": 1,
          "title": "Required",
          "type": "array",
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-exact-classification-admission-predicate"
          }
        }
      },
      "required": [
        "required"
      ],
      "title": "TaggedAdmissionSelector",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "format": {
      "const": "stove0-departures/v1",
      "default": "stove0-departures/v1",
      "title": "Format",
      "type": "string"
    },
    "policies": {
      "default": [],
      "items": {
        "$ref": "#/$defs/DeparturePolicy"
      },
      "maxItems": 100,
      "title": "Policies",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-deployment-departure-catalog"
      }
    }
  },
  "title": "DepartureCatalog",
  "type": "object"
}
```

</details>
