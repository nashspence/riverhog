# schemas: _FileProvenanceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-fileprovenanceout:d3ce541439 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-60336dfbc1"></a>


### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| <a id="s-3d92c532e7"></a>1 | [CapturedCollectionFileProvenanceOut](schemas-capturedcollectionfileprovenanceout.md) |
| <a id="s-dd99c51581"></a>2 | [OmittedCollectionFileProvenanceOut](schemas-omittedcollectionfileprovenanceout.md) |

## Maintained corroboration

### Referenced contract dossiers

- [CapturedCollectionFileProvenanceOut](schemas-capturedcollectionfileprovenanceout.md)
- [OmittedCollectionFileProvenanceOut](schemas-omittedcollectionfileprovenanceout.md)

## Governing policies

- <a id="pa-bc72071f42"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/_FileProvenanceOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1bc71fe1eca29d749d7950ecb0a31e22bd1a68d42d7a613e0193edaa0006f17 -->

```json
{
  "anyOf": [
    {
      "$ref": "#/components/schemas/CapturedCollectionFileProvenanceOut"
    },
    {
      "$ref": "#/components/schemas/OmittedCollectionFileProvenanceOut"
    }
  ]
}
```

</details>
