# generated:riverhog-storage-adapter: DeletePrefixRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-delete-ac2392c700:710a653f9b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-87eda5641d"></a>

- <a id="s-c11f7cf1bf"></a>`type`: `"object"`
- <a id="s-333f7b98c5"></a>`additionalProperties`: `false`
- <a id="s-8b20b43b1e"></a>`required`: `["object_prefix"]`
- <a id="s-744fd87910"></a>`title`: `"DeletePrefixRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fd2bba5510"></a>`mode` | no | type="string"; const="all_versions"; default="all_versions" |  |
| <a id="s-96398ee657"></a>`object_prefix` | yes | type="string"; maxLength=4096; minLength=1 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=4096; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field object_prefix](#s-96398ee657) | `length · characters · contract_max` | shared above |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

- <a id="pa-fdd3ed77c6"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-e45db11bd3"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/DeletePrefixRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 26f1798a7e22e1a72e0c6932bfbe66dca3e80273a417c3e5c18fdc0e4bd59a3f -->

```json
{
  "additionalProperties": false,
  "properties": {
    "mode": {
      "const": "all_versions",
      "default": "all_versions",
      "title": "Mode",
      "type": "string"
    },
    "object_prefix": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Object Prefix",
      "type": "string"
    }
  },
  "required": [
    "object_prefix"
  ],
  "title": "DeletePrefixRequest",
  "type": "object"
}
```

</details>
