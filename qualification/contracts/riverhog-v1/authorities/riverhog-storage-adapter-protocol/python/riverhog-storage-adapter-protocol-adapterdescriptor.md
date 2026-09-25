# riverhog_storage_adapter_protocol.AdapterDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-adapterdescriptor:1c87f504a5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-65e1a436bb"></a>
- <a id="s-b5b2c11f15"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-728a5464c2"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-ee265a69aa"></a>`name`: `AdapterDescriptor`
- <a id="s-9419e0d1de"></a>`unit`: `export`

### Declared structure

- <a id="s-ea83e34bf9"></a>`kind`: `"class"`
- <a id="s-5503e8069a"></a>`signature`: `"\"(*, protocol: Literal['riverhog-storage-adapter/v1'] = 'riverhog-storage-adapter/v1', storage_incarnation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', ascii_only=None)], implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], read_mode: Literal['immediate', 'restore_required'], minimum_nonfinal_segment_bytes: PositiveDecimal, maximum_segment_bytes: PositiveDecimal \| None = None, maximum_segment_count: PositiveDecimal \| None = None) -> None\""`

#### Validated model schema

<a id="s-8b430ee967"></a>

- <a id="s-b14721e05d"></a>`type`: `"object"`
- <a id="s-df5e49bf4d"></a>`additionalProperties`: `false`
- <a id="s-5005f5c155"></a>`required`: `["storage_incarnation_id","implementation_id","implementation_version","read_mode","minimum_nonfinal_segment_bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7d113a0a6f"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0816c01412"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-2bf6e29577"></a>`maximum_segment_bytes` | no | anyOf=[([PositiveDecimal](#s-77d8f06d74)); (type="null")]; default=null |  |
| <a id="s-4de2d33675"></a>`maximum_segment_count` | no | anyOf=[([PositiveDecimal](#s-77d8f06d74)); (type="null")]; default=null |  |
| <a id="s-5b5c395f56"></a>`minimum_nonfinal_segment_bytes` | yes | [PositiveDecimal](#s-77d8f06d74) |  |
| <a id="s-c01ebffa91"></a>`protocol` | no | type="string"; const="riverhog-storage-adapter/v1"; default="riverhog-storage-adapter/v1" |  |
| <a id="s-f27205a52f"></a>`read_mode` | yes | type="string"; enum=["immediate","restore_required"] |  |
| <a id="s-7621da1f83"></a>`storage_incarnation_id` | yes | type="string"; pattern="^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$" |  |

##### Definitions

- [PositiveDecimal](#s-77d8f06d74)

##### <a id="s-77d8f06d74"></a>definition `PositiveDecimal`

- <a id="s-16b24716b5"></a>`type`: `"string"`
- <a id="s-c8569fb39d"></a>`pattern`: `"^[1-9][0-9]*(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [validate_segment_limits](riverhog-storage-adapter-protocol-adapterdescriptor-validate-segment-limits.md)

## Governing policies

- <a id="pa-77afa7ee79"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.AdapterDescriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7dd8c7d209fa4eb64539931bc381fa3157c62894886568920bcded5bdd00d51c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "PositiveDecimal": {
          "pattern": "^[1-9][0-9]*(?![\\s\\S])",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "implementation_version": {
          "maxLength": 120,
          "minLength": 1,
          "type": "string"
        },
        "maximum_segment_bytes": {
          "anyOf": [
            {
              "$ref": "#/$defs/PositiveDecimal"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "maximum_segment_count": {
          "anyOf": [
            {
              "$ref": "#/$defs/PositiveDecimal"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "minimum_nonfinal_segment_bytes": {
          "$ref": "#/$defs/PositiveDecimal"
        },
        "protocol": {
          "const": "riverhog-storage-adapter/v1",
          "default": "riverhog-storage-adapter/v1",
          "type": "string"
        },
        "read_mode": {
          "enum": [
            "immediate",
            "restore_required"
          ],
          "type": "string"
        },
        "storage_incarnation_id": {
          "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
          "type": "string"
        }
      },
      "required": [
        "storage_incarnation_id",
        "implementation_id",
        "implementation_version",
        "read_mode",
        "minimum_nonfinal_segment_bytes"
      ],
      "type": "object"
    },
    "signature": "\"(*, protocol: Literal['riverhog-storage-adapter/v1'] = 'riverhog-storage-adapter/v1', storage_incarnation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', ascii_only=None)], implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], read_mode: Literal['immediate', 'restore_required'], minimum_nonfinal_segment_bytes: PositiveDecimal, maximum_segment_bytes: PositiveDecimal | None = None, maximum_segment_count: PositiveDecimal | None = None) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "AdapterDescriptor",
  "unit": "export"
}
```

</details>
