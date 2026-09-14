# riverhog-ftp-custody: receipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-ftp-custody:riverhog-ftp-custody-receipt:b1010c6324 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-e5f2534f2d"></a>
- Document: `receipt`

### Document schema

<a id="s-17735bcaf1"></a>
- <a id="s-35b8fede2a"></a>`title`: FtpReceiptState
- <a id="s-ff823cb7b9"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9d7edc41a5"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a02010b383"></a>`claim_id` | yes | type="string"; minLength=1 |  |
| <a id="s-26fb5dc079"></a>`collection_id` | yes | type="integer"; minimum=1 |  |
| <a id="s-02b9d25461"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-fb0e5e377d"></a>`format` | yes | type="string"; const="riverhog-ftp-adapter-receipt/v1" |  |
| <a id="s-085dd88286"></a>`riverhog_receipt` | yes | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-a6edc25a15"></a>`source_event_id` | yes | type="string"; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [riverhog-ftp-custody durable-state identity](riverhog-ftp-custody-durable-state-identity.md)

## Governing policies

- <a id="pa-9da0040fcf"></a>[compatibility/durable-state/v1](../../../policies/index.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [state:riverhog-ftp-custody](../../../evidence/sources.md#src-54f88a3a47) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/state_contract.py`

### Machine authority

- `/external_contract/durable_state/owners/6/structure/units/3`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f364260e5a010db8b2aa8f5c51796183ebf798416a28cbe7b82b0b829e5635e6 -->

```json
{
  "id": "receipt",
  "kind": "json-document",
  "schema": {
    "additionalProperties": false,
    "properties": {
      "archive_root_sha256": {
        "pattern": "^[0-9a-f]{64}$",
        "title": "Archive Root Sha256",
        "type": "string"
      },
      "claim_id": {
        "minLength": 1,
        "title": "Claim Id",
        "type": "string"
      },
      "collection_id": {
        "minimum": 1,
        "title": "Collection Id",
        "type": "integer"
      },
      "content_identity": {
        "pattern": "^[0-9a-f]{64}$",
        "title": "Content Identity",
        "type": "string"
      },
      "format": {
        "const": "riverhog-ftp-adapter-receipt/v1",
        "title": "Format",
        "type": "string"
      },
      "riverhog_receipt": {
        "additionalProperties": true,
        "title": "Riverhog Receipt",
        "type": "object"
      },
      "source_event_id": {
        "minLength": 1,
        "title": "Source Event Id",
        "type": "string"
      }
    },
    "required": [
      "format",
      "claim_id",
      "source_event_id",
      "collection_id",
      "archive_root_sha256",
      "content_identity",
      "riverhog_receipt"
    ],
    "title": "FtpReceiptState",
    "type": "object"
  }
}
```
