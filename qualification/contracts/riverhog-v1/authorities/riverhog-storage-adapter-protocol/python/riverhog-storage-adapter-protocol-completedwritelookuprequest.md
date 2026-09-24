# riverhog_storage_adapter_protocol.CompletedWriteLookupRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-complet-92915a63c7:6d8815fa61 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d3d0e65ce0"></a>
- <a id="s-bd89ea24e3"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-f837b0436c"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-aa8062fbd2"></a>`name`: `CompletedWriteLookupRequest`
- <a id="s-d71c948c8c"></a>`unit`: `export`

### Declared structure

- <a id="s-151f9f31e1"></a>`kind`: `"class"`
- <a id="s-4e102cc2fb"></a>`signature`: `"\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], expected_bytes: PositiveDecimal, expected_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], expected_placement_policy: Literal['archive_default', 'immediate_default']) -> None\""`

#### Validated model schema

<a id="s-4836c37ef0"></a>

- <a id="s-ac31600127"></a>`type`: `"object"`
- <a id="s-a6bee3cd02"></a>`additionalProperties`: `false`
- <a id="s-1947ad74e8"></a>`required`: `["object_path","expected_bytes","expected_content_type","required_identity_assertions","expected_placement_policy"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5a818e43f0"></a>`expected_bytes` | yes | [PositiveDecimal](#s-4cbb400754) |  |
| <a id="s-4065844c2e"></a>`expected_content_type` | yes | type="string"; maxLength=255; minLength=1 |  |
| <a id="s-a56f294142"></a>`expected_placement_policy` | yes | type="string"; enum=["archive_default","immediate_default"] |  |
| <a id="s-83c616d94c"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1 |  |
| <a id="s-f23981c971"></a>`required_identity_assertions` | yes | type="object"; additionalProperties=(type="string"); maxProperties=64; x-riverhog-encoded-bytes-max=16384; x-riverhog-extent={"policy":"contract_max","reason":"bounded-object-identity-assertion-envelope"} |  |

##### Definitions

- [PositiveDecimal](#s-4cbb400754)

##### <a id="s-4cbb400754"></a>definition `PositiveDecimal`

- <a id="s-d3dfc0456e"></a>`type`: `"string"`
- <a id="s-80241d39d9"></a>`pattern`: `"^[1-9][0-9]*(?![\\s\\S])"`

## Maintained corroboration

### Related interface records

- [canonical_path](riverhog-storage-adapter-protocol-completedwritelookuprequest-canonical-path.md)
- [canonical_metadata](riverhog-storage-adapter-protocol-completedwritelookuprequest-canonical-metadata.md)

## Governing policies

- <a id="pa-9da211f6e9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.CompletedWriteLookupRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d329824d0f41a5ed80cd9a3d2f337df9f5edabbc9a6b92bbed591f77f068074e -->

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
        "expected_bytes": {
          "$ref": "#/$defs/PositiveDecimal"
        },
        "expected_content_type": {
          "maxLength": 255,
          "minLength": 1,
          "type": "string"
        },
        "expected_placement_policy": {
          "enum": [
            "archive_default",
            "immediate_default"
          ],
          "type": "string"
        },
        "object_path": {
          "maxLength": 4096,
          "minLength": 1,
          "type": "string"
        },
        "required_identity_assertions": {
          "additionalProperties": {
            "type": "string"
          },
          "maxProperties": 64,
          "type": "object",
          "x-riverhog-encoded-bytes-max": 16384,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-object-identity-assertion-envelope"
          }
        }
      },
      "required": [
        "object_path",
        "expected_bytes",
        "expected_content_type",
        "required_identity_assertions",
        "expected_placement_policy"
      ],
      "type": "object"
    },
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], expected_bytes: PositiveDecimal, expected_content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], expected_placement_policy: Literal['archive_default', 'immediate_default']) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "CompletedWriteLookupRequest",
  "unit": "export"
}
```

</details>
