# stove0_operator_contracts.TaggedAdmissionSelector

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-taggedadmissionselector:9bccec08cb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3ddad52ef2"></a>
- <a id="s-1d5cd85895"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-e926c0cab3"></a>`module`: `stove0_operator_contracts`
- <a id="s-5a7ecc67e1"></a>`name`: `TaggedAdmissionSelector`
- <a id="s-3d12f23e60"></a>`unit`: `export`

### Declared structure

- <a id="s-6132e0baa4"></a>`kind`: `"class"`
- <a id="s-24beca60fc"></a>`signature`: `"\"(*, kind: Literal['tags'] = 'tags', required: Annotated[tuple[CollectionTag, ...], MinLen(min_length=1), MaxLen(max_length=100)]) -> None\""`

#### Validated model schema

<a id="s-5f9aec055a"></a>

- <a id="s-8664a81917"></a>`type`: `"object"`
- <a id="s-53c7dab373"></a>`additionalProperties`: `false`
- <a id="s-42a4a4a597"></a>`required`: `["required"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c9bb3e7732"></a>`kind` | no | type="string"; const="tags"; default="tags" |  |
| <a id="s-7cb7fde989"></a>`required` | yes | type="array"; items=([CollectionTag](#s-a4f2037d5b)); maxItems=100; minItems=1; x-riverhog-extent={"policy":"contract_max","reason":"bounded-exact-classification-admission-predicate"} |  |

##### Definitions

- [CollectionTag](#s-a4f2037d5b)

##### <a id="s-a4f2037d5b"></a>definition `CollectionTag`

- <a id="s-7342f22842"></a>`type`: `"string"`
- <a id="s-37e19f4982"></a>`maxLength`: `65536`
- <a id="s-6f4ee535ce"></a>`minLength`: `1`
- <a id="s-96220181b8"></a>`x-riverhog-encoded-bytes-max`: `65536`
- <a id="s-0a1c55a8fc"></a>`x-riverhog-extent`: `{"policy":"contract_max","reason":"bounded-human-authored-collection-tag"}`
- <a id="s-36c5132e36"></a>`x-unicode-normalization`: `"NFC"`

## Maintained corroboration

### Related interface records

- [canonical_required_tags](stove0-operator-contracts-taggedadmissionselector-canonical-required-tags.md)

## Governing policies

- <a id="pa-3af79a3d7a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.TaggedAdmissionSelector`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 083c2272581117c4cc43454ea6eb6c258567ec0575452f7e660978e67bf7a80b -->

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
        }
      },
      "additionalProperties": false,
      "properties": {
        "kind": {
          "const": "tags",
          "default": "tags",
          "type": "string"
        },
        "required": {
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
        }
      },
      "required": [
        "required"
      ],
      "type": "object"
    },
    "signature": "\"(*, kind: Literal['tags'] = 'tags', required: Annotated[tuple[CollectionTag, ...], MinLen(min_length=1), MaxLen(max_length=100)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "TaggedAdmissionSelector",
  "unit": "export"
}
```

</details>
