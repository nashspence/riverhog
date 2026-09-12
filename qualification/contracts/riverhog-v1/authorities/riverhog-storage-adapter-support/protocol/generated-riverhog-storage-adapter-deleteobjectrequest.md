# generated:riverhog-storage-adapter: DeleteObjectRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-delete-877fb953a2:b72fa48ed8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-4244e1c1b1"></a>
- <a id="s-1e3aee157a"></a>`title`: DeleteObjectRequest
- <a id="s-f7bf1217a9"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-25cdfe5616"></a>`expected_current_stored_sha256` | no | anyOf=type="string"; pattern="^[0-9a-f]{64}$" \| type="null" |  |
| <a id="s-e096d56146"></a>`mode` | yes | type="string"; enum=["current","exact_revision","all_versions"] |  |
| <a id="s-9578123839"></a>`object` | yes | #/$defs/ObjectLocator |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-1c064e0ef6"></a>`ObjectLocator` | type="object"; fields=`object_path`, `revision`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-6a0a486f31"></a>[field expected_current_stored_sha256 · string value](#s-25cdfe5616) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-e96704bca9"></a>[definition ObjectLocator · field object_path](#s-1c064e0ef6) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| <a id="s-f5db287c99"></a>[definition ObjectLocator · field revision · string value](#s-1c064e0ef6) | `length · characters · contract_max` | maximum=2000; minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-f55d9d5808"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-48d6c4f820"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/DeleteObjectRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 46f15404c356f50e3a381b04f209d8f64b9114cce82ef9e8c42967f06b59fad8 -->

```json
{
  "$defs": {
    "ObjectLocator": {
      "additionalProperties": false,
      "properties": {
        "object_path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Object Path",
          "type": "string"
        },
        "revision": {
          "anyOf": [
            {
              "maxLength": 2000,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Revision"
        }
      },
      "required": [
        "object_path"
      ],
      "title": "ObjectLocator",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "expected_current_stored_sha256": {
      "anyOf": [
        {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Expected Current Stored Sha256"
    },
    "mode": {
      "enum": [
        "current",
        "exact_revision",
        "all_versions"
      ],
      "title": "Mode",
      "type": "string"
    },
    "object": {
      "$ref": "#/$defs/ObjectLocator"
    }
  },
  "required": [
    "object",
    "mode"
  ],
  "title": "DeleteObjectRequest",
  "type": "object"
}
```
