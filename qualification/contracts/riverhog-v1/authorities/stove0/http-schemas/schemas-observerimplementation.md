# schemas: ObserverImplementation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-observerimplementation:e2482340c9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-fe3badbf65"></a>

- <a id="s-51335bead8"></a>`type`: `"object"`
- <a id="s-5b20cbee72"></a>`additionalProperties`: `false`
- <a id="s-951348a52a"></a>`required`: `["id","version","source_revision","descriptor_sha256"]`
- <a id="s-f640904f3b"></a>`title`: `"ObserverImplementation"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d8c5b17fff"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-0331c52b64"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Id" |  |
| <a id="s-f5bcb2a081"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1"; title="Protocol" |  |
| <a id="s-3297eafb2a"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1; title="Source Revision" |  |
| <a id="s-990a1adba3"></a>`version` | yes | type="string"; maxLength=120; minLength=1; title="Version" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field descriptor_sha256](#s-d8c5b17fff) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field source_revision](#s-3297eafb2a) | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |
| [field version](#s-990a1adba3) | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-b0e5ccca90"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-cc1ab4726c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObserverImplementation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ffdaa644c4d7b3d7f3e17d66ebec04a5997be6e6e59c8839ae0685b1250ba75 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "descriptor_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Descriptor Sha256",
      "type": "string"
    },
    "id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "protocol": {
      "const": "stove0-content-observer/v1",
      "default": "stove0-content-observer/v1",
      "title": "Protocol",
      "type": "string"
    },
    "source_revision": {
      "maxLength": 200,
      "minLength": 1,
      "title": "Source Revision",
      "type": "string"
    },
    "version": {
      "maxLength": 120,
      "minLength": 1,
      "title": "Version",
      "type": "string"
    }
  },
  "required": [
    "id",
    "version",
    "source_revision",
    "descriptor_sha256"
  ],
  "title": "ObserverImplementation",
  "type": "object"
}
```

</details>
