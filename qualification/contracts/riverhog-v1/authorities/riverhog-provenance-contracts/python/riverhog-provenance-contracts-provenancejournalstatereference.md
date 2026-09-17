# riverhog_provenance_contracts.ProvenanceJournalStateReference

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-contracts:riverhog-provenance-contracts-provenancej-0e2a1b02ae:f871a851d3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4b0964df26"></a>
- <a id="s-0f5d21e7dc"></a>`distribution`: `riverhog-provenance-contracts`
- <a id="s-78eb5c9244"></a>`module`: `riverhog_provenance_contracts`
- <a id="s-ea79346969"></a>`name`: `ProvenanceJournalStateReference`
- <a id="s-5bb7c3be33"></a>`unit`: `export`

### Declared structure

- <a id="s-d080ef4cec"></a>`kind`: `"class"`
- <a id="s-be36c935c8"></a>`signature`: `"'(*, journal_id: ProvenanceJournalId, current_state_id: ProvenanceStateId) -> None'"`

#### Validated model schema

<a id="s-17b24caaf5"></a>

- <a id="s-65e243e3dc"></a>`type`: `"object"`
- <a id="s-d3ff5b398a"></a>`additionalProperties`: `false`
- <a id="s-4a5d49abae"></a>`required`: `["journal_id","current_state_id"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bbe83a98ba"></a>`current_state_id` | yes | [ProvenanceStateId](#s-b300ca59b7) |  |
| <a id="s-22d8155f9c"></a>`journal_id` | yes | [ProvenanceJournalId](#s-647d53bd23) |  |

##### Definitions

- [ProvenanceJournalId](#s-647d53bd23)
- [ProvenanceStateId](#s-b300ca59b7)

##### <a id="s-647d53bd23"></a>definition `ProvenanceJournalId`

- <a id="s-5bf5818779"></a>`type`: `"string"`
- <a id="s-db6dd09519"></a>`pattern`: `"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"`

##### <a id="s-b300ca59b7"></a>definition `ProvenanceStateId`

- <a id="s-6df5827f61"></a>`type`: `"string"`
- <a id="s-db01f5d7a2"></a>`pattern`: `"^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"`

## Governing policies

- <a id="pa-ea38dba1d6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance-contracts:riverhog_provenance_contracts](../../../evidence/sources/authorities.md#src-9b6289a988) — [packages/riverhog-provenance-contracts/src/riverhog\_provenance\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance_contracts.ProvenanceJournalStateReference`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f44e58b988a432d0c51548f71b251c072dc6d0f164492238db92e2ec0fd5c1bf -->

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
        "current_state_id": {
          "$ref": "#/$defs/ProvenanceStateId"
        },
        "journal_id": {
          "$ref": "#/$defs/ProvenanceJournalId"
        }
      },
      "required": [
        "journal_id",
        "current_state_id"
      ],
      "type": "object"
    },
    "signature": "'(*, journal_id: ProvenanceJournalId, current_state_id: ProvenanceStateId) -> None'"
  },
  "distribution": "riverhog-provenance-contracts",
  "module": "riverhog_provenance_contracts",
  "name": "ProvenanceJournalStateReference",
  "unit": "export"
}
```

</details>
