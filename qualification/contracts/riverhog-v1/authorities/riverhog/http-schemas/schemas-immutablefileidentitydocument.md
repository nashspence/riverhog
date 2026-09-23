# schemas: ImmutableFileIdentityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-immutablefileidentitydocument:5d2a6a5732 -->

The exact path, length, and plaintext digest shared by file projections.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-62880c732d"></a>

- <a id="s-0fbaa2401b"></a>`type`: `"object"`
- <a id="s-bc8641ea45"></a>`additionalProperties`: `false`
- <a id="s-b9386adb4c"></a>`description`: `"The exact path, length, and plaintext digest shared by file projections."`
- <a id="s-7f5848f6cb"></a>`required`: `["path","bytes","sha256"]`
- <a id="s-b6ebf3dc41"></a>`title`: `"ImmutableFileIdentityDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-61eaa756b8"></a>`bytes` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md) |  |
| <a id="s-a970aa6e53"></a>`path` | yes | [CanonicalRelPath](schemas-canonicalrelpath.md) |  |
| <a id="s-7a011aa8fc"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256](#s-7a011aa8fc) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [CanonicalRelPath](schemas-canonicalrelpath.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c8ada22945"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-52de4da534"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ImmutableFileIdentityDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eeaa8d043ebe8f0003d4de21f05056dc6cd4864a7b7ab90b15eeacb75b3ec2fe -->

```json
{
  "additionalProperties": false,
  "description": "The exact path, length, and plaintext digest shared by file projections.",
  "properties": {
    "bytes": {
      "$ref": "#/components/schemas/NonnegativeDecimal"
    },
    "path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "path",
    "bytes",
    "sha256"
  ],
  "title": "ImmutableFileIdentityDocument",
  "type": "object"
}
```

</details>
