# stove0_observer_protocol.CollectionRootRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-collectionrootref:758880d4c6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a54d7eda75"></a>
- <a id="s-e7d6d4ae31"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-22608d5b06"></a>`module`: `stove0_observer_protocol`
- <a id="s-9b7ec3d380"></a>`name`: `CollectionRootRef`
- <a id="s-e57eeb528d"></a>`unit`: `export`

### Declared structure

- <a id="s-62ed0c48f8"></a>`kind`: `"class"`
- <a id="s-d5eafe4ca2"></a>`signature`: `"\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-ed0de08b1b"></a>

- <a id="s-1c50648d72"></a>`type`: `"object"`
- <a id="s-a70a46637e"></a>`additionalProperties`: `false`
- <a id="s-fccab3838b"></a>`required`: `["collection_id","archive_root_sha256","content_identity"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5064282fcf"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ad23d8ea96"></a>`collection_id` | yes | [CollectionId](#s-1dd2434f75) |  |
| <a id="s-514b860a1d"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [CollectionId](#s-1dd2434f75)

##### <a id="s-1dd2434f75"></a>definition `CollectionId`

- <a id="s-6bdae019ea"></a>`type`: `"integer"`
- <a id="s-a206b42870"></a>`minimum`: `1`

## Maintained corroboration

### Related interface records

- [to_identity](stove0-observer-protocol-collectionrootref-to-identity.md)
- [from_identity](stove0-observer-protocol-collectionrootref-from-identity.md)

## Governing policies

- <a id="pa-9e1fe6c93a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.CollectionRootRef`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5d7b0c8f62f5fc5f6d9014b0e9c864faa50552330960711c53e7eb1784a7824d -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        }
      },
      "additionalProperties": false,
      "properties": {
        "archive_root_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "content_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "archive_root_sha256",
        "content_identity"
      ],
      "type": "object"
    },
    "signature": "\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "CollectionRootRef",
  "unit": "export"
}
```

</details>
