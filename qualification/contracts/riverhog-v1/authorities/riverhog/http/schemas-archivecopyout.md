# schemas: ArchiveCopyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyout:f690c2df76 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b315c612f631"></a>
- <a id="s-ef910131ef2d"></a>`title`: ArchiveCopyOut

## Maintained corroboration

### Referenced contract dossiers

- [schemas: FailedArchiveCopyOut](schemas-failedarchivecopyout.md)
- [schemas: IncompleteArchiveCopyOut](schemas-incompletearchivecopyout.md)
- [schemas: UploadedArchiveCopyOut](schemas-uploadedarchivecopyout.md)

## Governing policies

- <a id="pa-3955662a7602"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 978aa8902a1ae7506a429f1503173c2cdfaa37ba4d366b316b2ab3a8a939c385 -->

```json
{
  "discriminator": {
    "mapping": {
      "failed": "#/components/schemas/FailedArchiveCopyOut",
      "pending": "#/components/schemas/IncompleteArchiveCopyOut",
      "retrying": "#/components/schemas/IncompleteArchiveCopyOut",
      "uploaded": "#/components/schemas/UploadedArchiveCopyOut",
      "uploading": "#/components/schemas/IncompleteArchiveCopyOut"
    },
    "propertyName": "state"
  },
  "oneOf": [
    {
      "$ref": "#/components/schemas/IncompleteArchiveCopyOut"
    },
    {
      "$ref": "#/components/schemas/UploadedArchiveCopyOut"
    },
    {
      "$ref": "#/components/schemas/FailedArchiveCopyOut"
    }
  ],
  "title": "ArchiveCopyOut"
}
```
