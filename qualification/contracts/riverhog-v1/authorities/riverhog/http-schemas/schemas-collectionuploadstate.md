# schemas: CollectionUploadState

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadstate:137aa0c4f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-6a88090d90"></a>

- <a id="s-468ca16847"></a>`type`: `"string"`
- <a id="s-9c1cda34c2"></a>`enum`: `["open","closing","uploading","finalizing","orphaned","discarding"]`

## Governing policies

- <a id="pa-169ae04a9b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadState`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: efeaf1d8deed1432b03d91b6725b0f08268a3051c221728fea931f4e8ec1a1ef -->

```json
{
  "enum": [
    "open",
    "closing",
    "uploading",
    "finalizing",
    "orphaned",
    "discarding"
  ],
  "type": "string"
}
```

</details>
