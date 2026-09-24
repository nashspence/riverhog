# riverhog_protocol.ProcessingCapabilityCreateDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingcapabilitycreatedocument:35c3123097 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-33692950d3"></a>
- <a id="s-f5a737b278"></a>`distribution`: `riverhog-protocol`
- <a id="s-8d55fd07fc"></a>`module`: `riverhog_protocol`
- <a id="s-16a92a99ff"></a>`name`: `ProcessingCapabilityCreateDocument`
- <a id="s-23334b34ce"></a>`unit`: `export`

### Declared structure

- <a id="s-739cd82314"></a>`kind`: `"class"`
- <a id="s-233989ee0b"></a>`signature`: `"\"(*, fence: Annotated[NonnegativeDecimal, Ge(ge=1)], audience: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9][a-z0-9._:/-]{0,299}$')], actions: Annotated[list[Literal['read-inputs', 'write-output']], MinLen(min_length=1)] = <factory>, ttl_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 900) -> None\""`

#### Validated model schema

<a id="s-5643e9b734"></a>

- <a id="s-15722fe96f"></a>`type`: `"object"`
- <a id="s-e551f6d822"></a>`additionalProperties`: `false`
- <a id="s-195b239a74"></a>`required`: `["fence","audience"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1b14f72423"></a>`actions` | no | type="array"; items=(type="string"; enum=["read-inputs","write-output"]); minItems=1; oneOf=[(const=["read-inputs"]); (const=["read-inputs","write-output"])] |  |
| <a id="s-de45c9e3e7"></a>`audience` | yes | type="string"; pattern="^[a-z0-9][a-z0-9._:/-]{0,299}$" |  |
| <a id="s-d11791bd4e"></a>`fence` | yes | [NonnegativeDecimal](#s-cdc2ea96e6); ge=1 |  |
| <a id="s-0bf8446067"></a>`ttl_seconds` | no | type="integer"; minimum=30; maximum=86400; default=900 |  |

##### Definitions

- [NonnegativeDecimal](#s-cdc2ea96e6)

##### <a id="s-cdc2ea96e6"></a>definition `NonnegativeDecimal`

- <a id="s-d7647c5505"></a>`type`: `"string"`
- <a id="s-fd9f7338d7"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [validate_capability](riverhog-protocol-processingcapabilitycreatedocument-validate-capability.md)
- [get](riverhog-protocol-processingcapabilitycreatedocument-get.md)
- [__getitem__](riverhog-protocol-processingcapabilitycreatedocument-getitem.md)

## Governing policies

- <a id="pa-35249addc9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingCapabilityCreateDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 85b87a7ad862040847ecc98835a4291102bb94cf3ce7527d68157799c7c225b7 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "NonnegativeDecimal": {
          "pattern": "^(?:0|[1-9][0-9]*)(?![\\s\\S])",
          "type": "string"
        }
      },
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
          "$ref": "#/$defs/NonnegativeDecimal",
          "ge": 1
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
    "signature": "\"(*, fence: Annotated[NonnegativeDecimal, Ge(ge=1)], audience: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9][a-z0-9._:/-]{0,299}$')], actions: Annotated[list[Literal['read-inputs', 'write-output']], MinLen(min_length=1)] = <factory>, ttl_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 900) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingCapabilityCreateDocument",
  "unit": "export"
}
```

</details>
