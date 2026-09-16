# riverhog_storage_adapter_protocol.AdapterDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-adapterdescriptor:1c87f504a5 -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-5503e8069a"></a>`signature`: `"\"(*, protocol: Literal['riverhog-storage-adapter/v1'] = 'riverhog-storage-adapter/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], read_mode: Literal['immediate', 'restore_required'], minimum_nonfinal_segment_bytes: Annotated[int, Ge(ge=1)], maximum_segment_bytes: Annotated[int \| None, Ge(ge=1)] = None, maximum_segment_count: Annotated[int \| None, Ge(ge=1)] = None) -> None\""`

#### Validated model schema

<a id="s-8b430ee967"></a>

- <a id="s-b14721e05d"></a>`type`: `"object"`
- <a id="s-df5e49bf4d"></a>`additionalProperties`: `false`
- <a id="s-5005f5c155"></a>`required`: `["implementation_id","implementation_version","read_mode","minimum_nonfinal_segment_bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7d113a0a6f"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-0816c01412"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-2bf6e29577"></a>`maximum_segment_bytes` | no | anyOf=(type="integer"; minimum=1) \| (type="null"); default=null |  |
| <a id="s-4de2d33675"></a>`maximum_segment_count` | no | anyOf=(type="integer"; minimum=1) \| (type="null"); default=null |  |
| <a id="s-5b5c395f56"></a>`minimum_nonfinal_segment_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-c01ebffa91"></a>`protocol` | no | type="string"; const="riverhog-storage-adapter/v1"; default="riverhog-storage-adapter/v1" |  |
| <a id="s-f27205a52f"></a>`read_mode` | yes | type="string"; enum=["immediate","restore_required"] |  |

## Maintained corroboration

### Related interface records

- [validate_segment_limits](riverhog-storage-adapter-protocol-adapterdescriptor-validate-segment-limits.md)

## Governing policies

- <a id="pa-77afa7ee79"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.AdapterDescriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 597ab4ef522c7f0615a9c10b04b26cb57367ae59a058fcd0528e35d0ec4d1604 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
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
              "minimum": 1,
              "type": "integer"
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
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "minimum_nonfinal_segment_bytes": {
          "minimum": 1,
          "type": "integer"
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
        }
      },
      "required": [
        "implementation_id",
        "implementation_version",
        "read_mode",
        "minimum_nonfinal_segment_bytes"
      ],
      "type": "object"
    },
    "signature": "\"(*, protocol: Literal['riverhog-storage-adapter/v1'] = 'riverhog-storage-adapter/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], read_mode: Literal['immediate', 'restore_required'], minimum_nonfinal_segment_bytes: Annotated[int, Ge(ge=1)], maximum_segment_bytes: Annotated[int | None, Ge(ge=1)] = None, maximum_segment_count: Annotated[int | None, Ge(ge=1)] = None) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "AdapterDescriptor",
  "unit": "export"
}
```

</details>
