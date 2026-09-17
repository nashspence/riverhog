# riverhog_protocol.CollectionUploadProvenanceJournalStatusDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadprovena-39769e4d6c:a198a9296c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cc38a2e4c5"></a>
- <a id="s-d195a089ff"></a>`distribution`: `riverhog-protocol`
- <a id="s-ad8af71d6b"></a>`module`: `riverhog_protocol`
- <a id="s-94fa29b8f6"></a>`name`: `CollectionUploadProvenanceJournalStatusDocument`
- <a id="s-04174ca703"></a>`unit`: `export`

### Declared structure

- <a id="s-6242748ea8"></a>`kind`: `"class"`
- <a id="s-0e976206ae"></a>`signature`: `"\"(*, journal_id: ProvenanceJournalId, state: Literal['accepting', 'validating', 'sealed', 'failed'], bytes: Annotated[int, Strict(strict=True), Ge(ge=1)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], accepted_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], failure: str \| None = None, current_state_id: ProvenanceStateId \| None = None, current_path: str \| None = None, current_bytes: Annotated[int \| None, Strict(strict=True), Ge(ge=0)] = None, current_sha256: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')])]] = None) -> None\""`

#### Validated model schema

<a id="s-48d89948a0"></a>

- <a id="s-e104228c65"></a>`type`: `"object"`
- <a id="s-f38b7aa55c"></a>`additionalProperties`: `false`
- <a id="s-610da3d15a"></a>`required`: `["journal_id","state","bytes","sha256","accepted_bytes"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c05f142da4"></a>`accepted_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-e0c87204a2"></a>`bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-ddef0dd98c"></a>`current_bytes` | no | anyOf=[(type="integer"; minimum=0); (type="null")]; default=null |  |
| <a id="s-c34cd2178b"></a>`current_path` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-7f2707a86d"></a>`current_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-232feea01f"></a>`current_state_id` | no | anyOf=[([ProvenanceStateId](#s-0e1edefe0b)); (type="null")]; default=null |  |
| <a id="s-7e7ede4480"></a>`failure` | no | anyOf=[(type="string"); (type="null")]; default=null |  |
| <a id="s-f7ea842030"></a>`journal_id` | yes | [ProvenanceJournalId](#s-941b870066) |  |
| <a id="s-8e2556666a"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5b08c1e75b"></a>`state` | yes | type="string"; enum=["accepting","validating","sealed","failed"] |  |

##### Definitions

- [ProvenanceJournalId](#s-941b870066)
- [ProvenanceStateId](#s-0e1edefe0b)

##### <a id="s-941b870066"></a>definition `ProvenanceJournalId`

- <a id="s-8f65b2da52"></a>`type`: `"string"`
- <a id="s-61d1c74f38"></a>`pattern`: `"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"`

##### <a id="s-0e1edefe0b"></a>definition `ProvenanceStateId`

- <a id="s-6a83d7fa83"></a>`type`: `"string"`
- <a id="s-bd5d0aa10d"></a>`pattern`: `"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"`

## Maintained corroboration

### Related interface records

- [validate_progress](riverhog-protocol-collectionuploadprovenancejournalstatusdocument-validate-progress.md)

## Governing policies

- <a id="pa-2e71d63453"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadProvenanceJournalStatusDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 38ec67178c1f9d556b73b32bd2a754c200eeb7aec8e4a3b287a25173326fb995 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ProvenanceJournalId": {
          "pattern": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
          "type": "string"
        },
        "ProvenanceStateId": {
          "pattern": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
          "type": "string"
        }
      },
      "additionalProperties": false,
      "properties": {
        "accepted_bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "current_bytes": {
          "anyOf": [
            {
              "minimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "current_path": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "current_sha256": {
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
        "current_state_id": {
          "anyOf": [
            {
              "$ref": "#/$defs/ProvenanceStateId"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "failure": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "journal_id": {
          "$ref": "#/$defs/ProvenanceJournalId"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "state": {
          "enum": [
            "accepting",
            "validating",
            "sealed",
            "failed"
          ],
          "type": "string"
        }
      },
      "required": [
        "journal_id",
        "state",
        "bytes",
        "sha256",
        "accepted_bytes"
      ],
      "type": "object"
    },
    "signature": "\"(*, journal_id: ProvenanceJournalId, state: Literal['accepting', 'validating', 'sealed', 'failed'], bytes: Annotated[int, Strict(strict=True), Ge(ge=1)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], accepted_bytes: Annotated[int, Strict(strict=True), Ge(ge=0)], failure: str | None = None, current_state_id: ProvenanceStateId | None = None, current_path: str | None = None, current_bytes: Annotated[int | None, Strict(strict=True), Ge(ge=0)] = None, current_sha256: Optional[Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')])]] = None) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadProvenanceJournalStatusDocument",
  "unit": "export"
}
```

</details>
