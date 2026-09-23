# riverhog-ftp-custody: receipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-ftp-custody:riverhog-ftp-custody-receipt:b1010c6324 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-e5f2534f2d"></a>

- Document: `receipt`

### Document schema

- `kind`: `"json-document"`
<a id="s-17735bcaf1"></a>

- <a id="s-ff823cb7b9"></a>`type`: `"object"`
- <a id="s-29ba16c9b6"></a>`additionalProperties`: `false`
- <a id="s-cdfb368732"></a>`required`: `["format","claim_id","source_event_id","collection_id","archive_root_sha256","content_identity","riverhog_receipt"]`
- <a id="s-35b8fede2a"></a>`title`: `"FtpReceiptState"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9d7edc41a5"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-a02010b383"></a>`claim_id` | yes | type="string"; minLength=1; title="Claim Id" |  |
| <a id="s-26fb5dc079"></a>`collection_id` | yes | [CollectionId](#s-fa11e063f9) |  |
| <a id="s-02b9d25461"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |
| <a id="s-fb0e5e377d"></a>`format` | yes | type="string"; const="riverhog-ftp-adapter-receipt/v1"; title="Format" |  |
| <a id="s-085dd88286"></a>`riverhog_receipt` | yes | type="object"; additionalProperties=(any JSON value); title="Riverhog Receipt" |  |
| <a id="s-a6edc25a15"></a>`source_event_id` | yes | type="string"; minLength=1; title="Source Event Id" |  |

#### Definitions

- [CollectionId](#s-fa11e063f9)

#### <a id="s-fa11e063f9"></a>definition `CollectionId`


##### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-d37124a68c"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-24935dac01"></a>2 | not=(const="0") |

## Maintained corroboration

### Related interface records

- [Schema identity](riverhog-ftp-custody-durable-state-identity.md)

## Governing policies

- <a id="pa-9da0040fcf"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:riverhog-ftp-custody](../../../evidence/sources/authorities.md#src-54f88a3a47) — [reference/riverhog/ingress/ftp/src/riverhog\_ftp\_adapter/state\_contract.py::ftp\_custody\_state\_contract](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/state_contract.py)

### Machine authority

- `/external_contract/durable_state/owners/6/structure/units/3`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7526f85189d52134779eccfc8f0b5c5b0a03a47213c542c0be6ba7db0c4ea970 -->

```json
{
  "id": "receipt",
  "kind": "json-document",
  "schema": {
    "$defs": {
      "CollectionId": {
        "allOf": [
          {
            "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
            "type": "string"
          },
          {
            "not": {
              "const": "0"
            }
          }
        ]
      }
    },
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
        "$ref": "#/$defs/CollectionId"
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

</details>
