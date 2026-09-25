# a-riverhog-ftp-spool-custody: receipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:a-riverhog-ftp-spool-custody:a-riverhog-ftp-spool-custody-receipt:a0356e73f6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool-custody](../index.md) |
| Interface | [Durable State](index.md) |

## External contract

<a id="s-486c778eb0"></a>

- Document: `receipt`

### Document schema

- `kind`: `"json-document"`
<a id="s-9a63fe3830"></a>

- <a id="s-3acc71663e"></a>`type`: `"object"`
- <a id="s-60899f10c0"></a>`additionalProperties`: `false`
- <a id="s-7aacb42c83"></a>`required`: `["format","claim_id","source_event_id","collection_id","archive_root_sha256","content_identity","riverhog_receipt"]`
- <a id="s-7017a0d9c4"></a>`title`: `"FtpReceiptState"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-660b49138f"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-647ea8b010"></a>`claim_id` | yes | type="string"; minLength=1; title="Claim Id" |  |
| <a id="s-aa03e1b292"></a>`collection_id` | yes | [CollectionId](#s-5d80024177) |  |
| <a id="s-0750752ce5"></a>`content_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Content Identity" |  |
| <a id="s-c23e998a72"></a>`format` | yes | type="string"; const="a-riverhog-ftp-spool-receipt/v1"; title="Format" |  |
| <a id="s-b5e201d141"></a>`riverhog_receipt` | yes | type="object"; additionalProperties=(any JSON value); title="Riverhog Receipt" |  |
| <a id="s-dafb646060"></a>`source_event_id` | yes | type="string"; minLength=1; title="Source Event Id" |  |

#### Definitions

- [CollectionId](#s-5d80024177)

#### <a id="s-5d80024177"></a>definition `CollectionId`


##### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-2dbe0b45da"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-512f3a92ca"></a>2 | not=(const="0") |

## Maintained corroboration

### Related interface records

- [Schema identity](a-riverhog-ftp-spool-custody-durable-state-identity.md)

## Governing policies

- <a id="pa-42c7b391a9"></a>[compatibility/durable-state/v1](../../release/compatibility-guarantees/compatibility-durable-state.md#p-214a49c2de)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make database-qualification](../../../evidence/sources/commands.md#q-27f281b51e)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [state:a-riverhog-ftp-spool-custody](../../../evidence/sources/authorities.md#src-97232522a5) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/state\_contract.py::ftp\_custody\_state\_contract](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/state_contract.py)

### Machine authority

- `/external_contract/durable_state/owners/8/structure/units/3`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 479ac5f7fed2ff8980b3670a73e37bfd3beaafc1b478506ab1958cc657c0d739 -->

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
        "const": "a-riverhog-ftp-spool-receipt/v1",
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
