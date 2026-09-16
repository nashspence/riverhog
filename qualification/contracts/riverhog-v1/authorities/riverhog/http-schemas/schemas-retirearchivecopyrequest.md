# schemas: RetireArchiveCopyRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-retirearchivecopyrequest:e39f70dac1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-dce794ca6f"></a>

- <a id="s-fac5e985e3"></a>`type`: `"object"`
- <a id="s-ac67c52c18"></a>`additionalProperties`: `false`
- <a id="s-81fe1ce039"></a>`required`: `["collection_id","store","challenge"]`
- <a id="s-dba0137491"></a>`title`: `"RetireArchiveCopyRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-297a890b93"></a>`challenge` | yes | type="string"; title="Challenge" |  |
| <a id="s-6f7ff50dd5"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-db0734895d"></a>`store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |

## Maintained corroboration

### Referenced contract dossiers

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-1d43522f70"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RetireArchiveCopyRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a7657b155bbfe998f725e6bba8a5ee650a6dca5b5543e08b474e65648b4a4eae -->

```json
{
  "additionalProperties": false,
  "properties": {
    "challenge": {
      "title": "Challenge",
      "type": "string"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    }
  },
  "required": [
    "collection_id",
    "store",
    "challenge"
  ],
  "title": "RetireArchiveCopyRequest",
  "type": "object"
}
```

</details>
