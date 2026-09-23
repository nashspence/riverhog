# generated:riverhog-storage-adapter: WriteSession

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writesession:54ab7331ec -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-5d276c77f5"></a>

- <a id="s-514d7def0e"></a>`type`: `"object"`
- <a id="s-bee685210f"></a>`additionalProperties`: `false`
- <a id="s-80cb8c0bda"></a>`required`: `["object_path","expected_bytes","write_token"]`
- <a id="s-9a6acf1bee"></a>`title`: `"WriteSession"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b573b398f3"></a>`expected_bytes` | yes | [PositiveDecimal](#s-6d84db3c97) | Exact immutable-object byte length admitted by this write session. The value remains fixed until the write becomes terminal. |
| <a id="s-f382e94026"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-bb12b9b006"></a>`write_token` | yes | type="string"; maxLength=4000; minLength=1; title="Write Token" | Opaque adapter-owned persistable continuation handle. For the same configured adapter it remains replayable across client, transport, Riverhog, and adapter process restarts until completion, explicit abort, or caller-authorized incomplete-write reclamation makes the write terminal. |

### Definitions

- [PositiveDecimal](#s-6d84db3c97)

### <a id="s-6d84db3c97"></a>definition `PositiveDecimal`

- <a id="s-57c6d9f62d"></a>`type`: `"string"`
- <a id="s-e9e157f732"></a>`pattern`: `"^[1-9][0-9]*(?![\\s\\S])"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field object_path](#s-f382e94026) | `length · characters · contract_max` | maximum=4096 |
| [field write_token](#s-bb12b9b006) | `length · characters · contract_max` | maximum=4000 |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-11441d886d"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-38ceaab584"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteSession`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56b03e966c6aa7128fb52d098f2ffb424937df0f91ddfaa786b2d30934b759cf -->

```json
{
  "$defs": {
    "PositiveDecimal": {
      "pattern": "^[1-9][0-9]*(?![\\s\\S])",
      "type": "string"
    }
  },
  "additionalProperties": false,
  "properties": {
    "expected_bytes": {
      "$ref": "#/$defs/PositiveDecimal",
      "description": "Exact immutable-object byte length admitted by this write session. The value remains fixed until the write becomes terminal."
    },
    "object_path": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Object Path",
      "type": "string"
    },
    "write_token": {
      "description": "Opaque adapter-owned persistable continuation handle. For the same configured adapter it remains replayable across client, transport, Riverhog, and adapter process restarts until completion, explicit abort, or caller-authorized incomplete-write reclamation makes the write terminal.",
      "maxLength": 4000,
      "minLength": 1,
      "title": "Write Token",
      "type": "string"
    }
  },
  "required": [
    "object_path",
    "expected_bytes",
    "write_token"
  ],
  "title": "WriteSession",
  "type": "object"
}
```

</details>
