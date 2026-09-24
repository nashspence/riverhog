# schemas: CollectionFileProvenanceTraceOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionfileprovenancetraceout:6971020550 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-160da0fc8a"></a>

- <a id="s-b6f5784b06"></a>`title`: `"CollectionFileProvenanceTraceOut"`

### At least one must match (`anyOf`)

| Alternative | Schema |
|---|---|
| <a id="s-b825bef221"></a>1 | [CapturedCollectionFileProvenanceTraceOut](schemas-capturedcollectionfileprovenancetraceout.md) |
| <a id="s-8b9e056f8f"></a>2 | [OmittedCollectionFileProvenanceTraceOut](schemas-omittedcollectionfileprovenancetraceout.md) |

## Maintained corroboration

### Referenced contract elements

- [CapturedCollectionFileProvenanceTraceOut](schemas-capturedcollectionfileprovenancetraceout.md)
- [OmittedCollectionFileProvenanceTraceOut](schemas-omittedcollectionfileprovenancetraceout.md)

## Governing policies

- <a id="pa-9257a3327e"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionFileProvenanceTraceOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ffae236bd5ff4a1cb5d80e6bba10a6dda77ccd4a580c2fdee5bd6bee98d651a4 -->

```json
{
  "anyOf": [
    {
      "$ref": "#/components/schemas/CapturedCollectionFileProvenanceTraceOut"
    },
    {
      "$ref": "#/components/schemas/OmittedCollectionFileProvenanceTraceOut"
    }
  ],
  "title": "CollectionFileProvenanceTraceOut"
}
```

</details>
