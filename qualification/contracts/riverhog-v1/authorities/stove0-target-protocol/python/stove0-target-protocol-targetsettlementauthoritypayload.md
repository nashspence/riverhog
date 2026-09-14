# stove0_target_protocol.TargetSettlementAuthorityPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetsettlementau-61bbf99e56:636d04ed1b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-350d51121d"></a>
- <a id="s-cc6031559f"></a>`distribution`: `stove0-target-protocol`
- <a id="s-91700a557a"></a>`module`: `stove0_target_protocol`
- <a id="s-e6b072b23b"></a>`name`: `TargetSettlementAuthorityPayload`
- <a id="s-0e8755349b"></a>`unit`: `export`

### Declared structure

- <a id="s-1c3af33263"></a>`kind`: `"class"`
- <a id="s-268ab5b9a5"></a>`signature`: `"\"(*, format: Literal['stove0-target-settlement/v1'] = 'stove0-target-settlement/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], production_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_target_protocol.protocol.OutputCollectionRef, output_bindings: stove0_target_protocol.protocol.TargetOutputBindingSetIdentity) -> None\""`

#### Validated model schema

<a id="s-874c7f4608"></a>
- <a id="s-8264798fc4"></a>`title`: TargetSettlementAuthorityPayload
- <a id="s-0f5d1f42e6"></a>`description`: Stove0-owned post-root settlement over one immutable production authority.
- <a id="s-155bb0da2e"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-236dcd4b3f"></a>`format` | no | type="string"; const="stove0-target-settlement/v1" |  |
| <a id="s-50a9863786"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-09edab56a9"></a>`output_bindings` | yes | #/$defs/TargetOutputBindingSetIdentity |  |
| <a id="s-566f0729ca"></a>`output_collection` | yes | #/$defs/OutputCollectionRef |  |
| <a id="s-5ad8646018"></a>`production_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-2089952b87"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-3255c55702"></a>`OutputCollectionRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`, `derivation_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-897edc1cc2"></a>`TargetOutputBindingSetIdentity` | type="object"; fields=`artifact_count`, `sha256`, `total_bytes`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-a790c0249c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetSettlementAuthorityPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1fb0efc5698b0ac545e7de3d2c7fb7a94f20728745f4e153e28b2fb21852786 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "OutputCollectionRef": {
          "additionalProperties": false,
          "properties": {
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Archive Root Sha256",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Content Identity",
              "type": "string"
            },
            "derivation_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Derivation Sha256",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity",
            "derivation_sha256"
          ],
          "title": "OutputCollectionRef",
          "type": "object"
        },
        "TargetOutputBindingSetIdentity": {
          "additionalProperties": false,
          "properties": {
            "artifact_count": {
              "minimum": 1,
              "title": "Artifact Count",
              "type": "integer"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "title": "Sha256",
              "type": "string"
            },
            "total_bytes": {
              "minimum": 0,
              "title": "Total Bytes",
              "type": "integer"
            }
          },
          "required": [
            "artifact_count",
            "total_bytes",
            "sha256"
          ],
          "title": "TargetOutputBindingSetIdentity",
          "type": "object"
        }
      },
      "additionalProperties": false,
      "description": "Stove0-owned post-root settlement over one immutable production authority.",
      "properties": {
        "format": {
          "const": "stove0-target-settlement/v1",
          "default": "stove0-target-settlement/v1",
          "title": "Format",
          "type": "string"
        },
        "job_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Job Id",
          "type": "string"
        },
        "output_bindings": {
          "$ref": "#/$defs/TargetOutputBindingSetIdentity"
        },
        "output_collection": {
          "$ref": "#/$defs/OutputCollectionRef"
        },
        "production_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Production Sha256",
          "type": "string"
        }
      },
      "required": [
        "job_id",
        "production_sha256",
        "output_collection",
        "output_bindings"
      ],
      "title": "TargetSettlementAuthorityPayload",
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-target-settlement/v1'] = 'stove0-target-settlement/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], production_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_target_protocol.protocol.OutputCollectionRef, output_bindings: stove0_target_protocol.protocol.TargetOutputBindingSetIdentity) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetSettlementAuthorityPayload",
  "unit": "export"
}
```
