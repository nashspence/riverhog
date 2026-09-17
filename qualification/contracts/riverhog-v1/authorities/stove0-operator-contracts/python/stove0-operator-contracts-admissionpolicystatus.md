# stove0_operator_contracts.AdmissionPolicyStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-admissionpolicystatus:07533add16 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a53f1e89da"></a>
- <a id="s-18e3233808"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-9e6f450fff"></a>`module`: `stove0_operator_contracts`
- <a id="s-6608828010"></a>`name`: `AdmissionPolicyStatus`
- <a id="s-b35967e0e6"></a>`unit`: `export`

### Declared structure

- <a id="s-bedb584586"></a>`kind`: `"class"`
- <a id="s-83f2c0efa8"></a>`signature`: `"\"(*, policy: stove0_operator_contracts.AdmissionPolicy, policy_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['new', 'baseline', 'following', 'reset_required'], source_identity: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, authorization_view_identity: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, baseline_mode: Literal['observe', 'backfill'], through_revision: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:0\|[1-9][0-9]*)$')], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None\""`

#### Validated model schema

<a id="s-3203bf160f"></a>

- <a id="s-7c331f0fe3"></a>`type`: `"object"`
- <a id="s-4bf03473de"></a>`additionalProperties`: `false`
- <a id="s-7f17fe73ea"></a>`required`: `["policy","policy_sha256","phase","baseline_mode","through_revision","updated_at"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-61e92577bf"></a>`authorization_view_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-14107432f2"></a>`baseline_mode` | yes | type="string"; enum=["observe","backfill"] |  |
| <a id="s-b089560ded"></a>`phase` | yes | type="string"; enum=["new","baseline","following","reset_required"] |  |
| <a id="s-2df79306d7"></a>`policy` | yes | [AdmissionPolicy](#s-e72ad30da5) |  |
| <a id="s-aee0f3b523"></a>`policy_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-579dc64d48"></a>`source_identity` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-600e42a725"></a>`through_revision` | yes | type="string"; pattern="^(?:0\|[1-9][0-9]*)$" |  |
| <a id="s-3c47e8b3e9"></a>`updated_at` | yes | type="string"; maxLength=40; minLength=1 |  |

##### Definitions

- [AdmissionPolicy](#s-e72ad30da5)
- [CollectionTag](#s-0c9cd7d2a9)
- [JsonValue](#s-9f78c43b46)

##### <a id="s-e72ad30da5"></a>definition `AdmissionPolicy`

- <a id="s-0ab03be3e3"></a>`type`: `"object"`
- <a id="s-453661baa3"></a>`additionalProperties`: `false`
- <a id="s-c75bbb9457"></a>`required`: `["id","revision","required_tags","recipe_id","recipe_revision","recipe_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-93f67cc858"></a>`automatic_preview` | no | type="string"; const="accept-ready"; default="accept-ready" |  |
| <a id="s-c44d23e313"></a>`effective_intent` | no | type="object"; additionalProperties=([JsonValue](#s-9f78c43b46)) |  |
| <a id="s-ba4485ef68"></a>`format` | no | type="string"; const="stove0-admission-policy/v1"; default="stove0-admission-policy/v1" |  |
| <a id="s-c0bfde817a"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a3497eebd6"></a>`recipe_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-c1d6be1b7a"></a>`recipe_revision` | yes | type="integer"; minimum=1 |  |
| <a id="s-4c72733695"></a>`recipe_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ccdc9bb862"></a>`required_tags` | yes | type="array"; items=([CollectionTag](#s-0c9cd7d2a9)); maxItems=100; minItems=1; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |
| <a id="s-4b4241829d"></a>`revision` | yes | type="integer"; minimum=1 |  |

##### <a id="s-0c9cd7d2a9"></a>definition `CollectionTag`

- <a id="s-9d19f13776"></a>`type`: `"string"`
- <a id="s-9724c6370f"></a>`maxLength`: `65536`
- <a id="s-fe4030bbcf"></a>`minLength`: `1`
- <a id="s-3142ada5b5"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-00e281acc8"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-618add1af3"></a>`x-unicode-normalization`: `"NFC"`

##### <a id="s-9f78c43b46"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [exact_policy](stove0-operator-contracts-admissionpolicystatus-exact-policy.md)

## Governing policies

- <a id="pa-e69858dd95"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — [reference/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.AdmissionPolicyStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 41281c582e714c92834389590bfc5df77bd7f1f33df57b739ac99fbedcf7a484 -->

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
        "authorization_view_identity": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "baseline_mode": {
          "enum": [
            "observe",
            "backfill"
          ],
          "type": "string"
        },
        "phase": {
          "enum": [
            "new",
            "baseline",
            "following",
            "reset_required"
          ],
          "type": "string"
        },
        "policy": {
          "$ref": "#/$defs/AdmissionPolicy"
        },
        "policy_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "source_identity": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "through_revision": {
          "pattern": "^(?:0|[1-9][0-9]*)$",
          "type": "string"
        },
        "updated_at": {
          "maxLength": 40,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "policy",
        "policy_sha256",
        "phase",
        "baseline_mode",
        "through_revision",
        "updated_at"
      ],
      "type": "object"
    },
    "signature": "\"(*, policy: stove0_operator_contracts.AdmissionPolicy, policy_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['new', 'baseline', 'following', 'reset_required'], source_identity: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, authorization_view_identity: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, baseline_mode: Literal['observe', 'backfill'], through_revision: Annotated[str, _PydanticGeneralMetadata(pattern='^(?:0|[1-9][0-9]*)$')], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "AdmissionPolicyStatus",
  "unit": "export"
}
```

</details>
