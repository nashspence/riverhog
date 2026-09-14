# riverhog_protocol.TransformCapabilityCreateDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-transformcapabilitycreatedocument:0927b03139 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d6e7d0fcc2"></a>
- <a id="s-382ca82e8a"></a>`distribution`: `riverhog-protocol`
- <a id="s-8f590c17c3"></a>`module`: `riverhog_protocol`
- <a id="s-c22c117ca3"></a>`name`: `TransformCapabilityCreateDocument`
- <a id="s-866d47f293"></a>`unit`: `export`

### Declared structure

- <a id="s-5a1a97b314"></a>`kind`: `"class"`
- <a id="s-d171fa4ee0"></a>`signature`: `"\"(*, fence: Annotated[int, Ge(ge=1)], audience: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9][a-z0-9._:/-]{0,299}$')], actions: Annotated[list[Literal['read-inputs', 'write-output']], MinLen(min_length=1)] = <factory>, ttl_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 900) -> None\""`

#### Validated model schema

<a id="s-6b6311c3b3"></a>
- <a id="s-8c8dc28740"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7b3788c2b2"></a>`actions` | no | type="array"; minItems=1; items=(type="string"; enum=["read-inputs","write-output"]); oneOf=const=["read-inputs"] \| const=["read-inputs","write-output"] |  |
| <a id="s-3677aee9dc"></a>`audience` | yes | type="string"; pattern="^[a-z0-9][a-z0-9._:/-]{0,299}$" |  |
| <a id="s-517200eda4"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-fbbcc79c6e"></a>`ttl_seconds` | no | type="integer"; minimum=30; maximum=86400 |  |

## Maintained corroboration

### Related interface records

- [validate_capability](riverhog-protocol-transformcapabilitycreatedocument-validate-capability.md)

## Governing policies

- <a id="pa-eedecf10b4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.TransformCapabilityCreateDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c6fa98c8d2a33a89d363f029347940eae45976cd3f7a6f8d4ba81b25366eeac3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "actions": {
          "items": {
            "enum": [
              "read-inputs",
              "write-output"
            ],
            "type": "string"
          },
          "minItems": 1,
          "oneOf": [
            {
              "const": [
                "read-inputs"
              ]
            },
            {
              "const": [
                "read-inputs",
                "write-output"
              ]
            }
          ],
          "type": "array"
        },
        "audience": {
          "pattern": "^[a-z0-9][a-z0-9._:/-]{0,299}$",
          "type": "string"
        },
        "fence": {
          "minimum": 1,
          "type": "integer"
        },
        "ttl_seconds": {
          "default": 900,
          "maximum": 86400,
          "minimum": 30,
          "type": "integer"
        }
      },
      "required": [
        "fence",
        "audience"
      ],
      "type": "object"
    },
    "signature": "\"(*, fence: Annotated[int, Ge(ge=1)], audience: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9][a-z0-9._:/-]{0,299}$')], actions: Annotated[list[Literal['read-inputs', 'write-output']], MinLen(min_length=1)] = <factory>, ttl_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 900) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "TransformCapabilityCreateDocument",
  "unit": "export"
}
```
