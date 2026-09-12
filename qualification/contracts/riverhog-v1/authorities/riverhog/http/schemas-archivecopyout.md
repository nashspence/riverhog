# schemas: ArchiveCopyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyout:f690c2df76 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyOut`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: FailedArchiveCopyOut](schemas-failedarchivecopyout.md)
- [schemas: IncompleteArchiveCopyOut](schemas-incompletearchivecopyout.md)
- [schemas: UploadedArchiveCopyOut](schemas-uploadedarchivecopyout.md)

## Contract summary

- `title`: ArchiveCopyOut

## Complete owned contract

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
