# generated:riverhog-storage-adapter: DeleteObjectRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-delete-877fb953a2:83b3270301 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-4244e1c1b1"></a>

- <a id="s-f7bf1217a9"></a>`type`: `"object"`
- <a id="s-25f878224d"></a>`additionalProperties`: `false`
- <a id="s-5cb80a641b"></a>`required`: `["object","mode"]`
- <a id="s-1e3aee157a"></a>`title`: `"DeleteObjectRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-25cdfe5616"></a>`expected_current_stored_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null; title="Expected Current Stored Sha256" |  |
| <a id="s-e096d56146"></a>`mode` | yes | type="string"; enum=["current","exact_revision","all_versions"]; title="Mode" |  |
| <a id="s-9578123839"></a>`object` | yes | [ObjectLocator](#s-1c064e0ef6) |  |

### Definitions

- [ObjectLocator](#s-1c064e0ef6)

### <a id="s-1c064e0ef6"></a>definition `ObjectLocator`

- <a id="s-dd8fd84a60"></a>`type`: `"object"`
- <a id="s-b6fc1dcfce"></a>`additionalProperties`: `false`
- <a id="s-df62ecfbd6"></a>`required`: `["object_path"]`
- <a id="s-dcd44e2bd5"></a>`title`: `"ObjectLocator"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e96704bca9"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-6c4d81adfa"></a>`revision` | no | anyOf=[(type="string"; maxLength=2000; minLength=1); (type="null")]; default=null; title="Revision" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-6a0a486f31"></a>[field expected_current_stored_sha256 · string value](#s-25cdfe5616) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition ObjectLocator · field object_path](#s-e96704bca9) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| <a id="s-f5db287c99"></a>[definition ObjectLocator · field revision · string value](#s-6c4d81adfa) | `length · characters · contract_max` | maximum=2000; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-22ba887384"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-7402a22b43"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/DeleteObjectRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
