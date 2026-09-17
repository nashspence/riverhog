# schemas: CatalogSyncCheckpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-catalogsynccheckpoint:af61271ac8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-9ddd462b8c"></a>

- <a id="s-b913c0e64e"></a>`type`: `"object"`
- <a id="s-4202917118"></a>`additionalProperties`: `false`
- <a id="s-edb6452966"></a>`required`: `["source_identity","authorization_view_identity","catalog_cursor"]`
- <a id="s-20a428ce84"></a>`title`: `"CatalogSyncCheckpoint"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-83cd2c8930"></a>`authorization_view_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$"; title="Authorization View Identity" |  |
| <a id="s-dc231f2428"></a>`catalog_cursor` | yes | type="string"; maxLength=4096; minLength=1; title="Catalog Cursor" |  |
| <a id="s-cc40445d90"></a>`format` | no | type="string"; const="riverhog-catalog-sync/v1"; default="riverhog-catalog-sync/v1"; title="Format" |  |
| <a id="s-e8505dafc9"></a>`source_identity` | yes | type="string"; maxLength=64; minLength=64; pattern="^[0-9a-f]{64}$"; title="Source Identity" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field authorization_view_identity](#s-83cd2c8930) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |
| [field catalog_cursor](#s-dc231f2428) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field source_identity](#s-e8505dafc9) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation" |

## Governing policies

- <a id="pa-ce592a819f"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-61250bf920"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CatalogSyncCheckpoint`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 23f1a9cd803ee925c19334eab9c8b6024bbbee97eb6be8c601b45924f2bf8d9e -->

```json
{
  "additionalProperties": false,
  "properties": {
    "authorization_view_identity": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Authorization View Identity",
      "type": "string"
    },
    "catalog_cursor": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Catalog Cursor",
      "type": "string"
    },
    "format": {
      "const": "riverhog-catalog-sync/v1",
      "default": "riverhog-catalog-sync/v1",
      "title": "Format",
      "type": "string"
    },
    "source_identity": {
      "maxLength": 64,
      "minLength": 64,
      "pattern": "^[0-9a-f]{64}$",
      "title": "Source Identity",
      "type": "string"
    }
  },
  "required": [
    "source_identity",
    "authorization_view_identity",
    "catalog_cursor"
  ],
  "title": "CatalogSyncCheckpoint",
  "type": "object"
}
```

</details>
