# schemas: ArtifactSubject

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-artifactsubject:4cb825bc38 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-76ac33c45c"></a>

- <a id="s-ef1b7e3fd1"></a>`type`: `"object"`
- <a id="s-d31d209bd6"></a>`additionalProperties`: `false`
- <a id="s-81f4a81810"></a>`required`: `["id","role","collection","path","bytes","sha256"]`
- <a id="s-34c5aa63aa"></a>`title`: `"ArtifactSubject"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d66284da83"></a>`bytes` | yes | type="integer"; minimum=0; title="Bytes" |  |
| <a id="s-ef263322d2"></a>`collection` | yes | [CollectionRootRef](schemas-collectionrootref.md) |  |
| <a id="s-c93b16de84"></a>`id` | yes | type="string"; pattern="^[A-Za-z0-9]&#40;?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"; title="Id" |  |
| <a id="s-90d6f2e055"></a>`media_type` | no | anyOf=[(type="string"; maxLength=255; minLength=1); (type="null")]; title="Media Type" |  |
| <a id="s-05f2ba23fe"></a>`path` | yes | type="string"; maxLength=4096; minLength=1; title="Path" |  |
| <a id="s-4b0dbbe13a"></a>`role` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"; title="Role" |  |
| <a id="s-b37980beb8"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Sha256" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field bytes](#s-d66284da83) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-3acf778414"></a>[field media_type · string value](#s-90d6f2e055) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field path](#s-05f2ba23fe) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field sha256](#s-b37980beb8) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [CollectionRootRef](schemas-collectionrootref.md)

## Governing policies

- <a id="pa-aae9877526"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-60d2e67e32"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-e50e5ce963"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ArtifactSubject`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: df08e6706de81bae01abd5d9a7e33127a419d69bdf0ce78158c1fb388a8ae354 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "collection": {
      "$ref": "#/components/schemas/CollectionRootRef"
    },
    "id": {
      "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
      "title": "Id",
      "type": "string"
    },
    "media_type": {
      "anyOf": [
        {
          "maxLength": 255,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Media Type"
    },
    "path": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Path",
      "type": "string"
    },
    "role": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Role",
      "type": "string"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "id",
    "role",
    "collection",
    "path",
    "bytes",
    "sha256"
  ],
  "title": "ArtifactSubject",
  "type": "object"
}
```

</details>
