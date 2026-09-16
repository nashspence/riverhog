# schemas: ArchiveCopyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyout:de6f1bf56f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-b315c612f6"></a>

- <a id="s-c6659fc9ec"></a>`discriminator`: `{"mapping":{"failed":"#/components/schemas/FailedArchiveCopyOut","pending":"#/components/schemas/IncompleteArchiveCopyOut","retrying":"#/components/schemas/IncompleteArchiveCopyOut","uploaded":"#/components/schemas/UploadedArchiveCopyOut","uploading":"#/components/schemas/IncompleteArchiveCopyOut"},"propertyName":"state"}`
- <a id="s-ef910131ef"></a>`title`: `"ArchiveCopyOut"`

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-c23c3acf2f"></a>1 | #/components/schemas/IncompleteArchiveCopyOut |
| <a id="s-ce48c65989"></a>2 | #/components/schemas/UploadedArchiveCopyOut |
| <a id="s-801c95d81d"></a>3 | #/components/schemas/FailedArchiveCopyOut |

## Maintained corroboration

### Referenced contract dossiers

- [FailedArchiveCopyOut](schemas-failedarchivecopyout.md)
- [IncompleteArchiveCopyOut](schemas-incompletearchivecopyout.md)
- [UploadedArchiveCopyOut](schemas-uploadedarchivecopyout.md)

## Governing policies

- <a id="pa-b9754e8f65"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
