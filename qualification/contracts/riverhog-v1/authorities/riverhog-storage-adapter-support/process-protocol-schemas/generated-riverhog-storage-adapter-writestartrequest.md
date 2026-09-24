# generated:riverhog-storage-adapter: WriteStartRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-writestartrequest:b2c3e6e6d6 -->

Exact authority for one idempotently established nonterminal write.

Repeating the same canonical request against the same configured adapter while the
write remains nonterminal returns the same continuation session. Operational
credentials used to realize that session remain adapter-private.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-2b36e52b84"></a>

- <a id="s-5de2718e1e"></a>`type`: `"object"`
- <a id="s-0ed83f3a4f"></a>`additionalProperties`: `false`
- <a id="s-5b9e227aa3"></a>`description`: `"Exact authority for one idempotently established nonterminal write.\n\nRepeating the same canonical request against the same configured adapter while the\nwrite remains nonterminal returns the same continuation session. Operational\ncredentials used to realize that session remain adapter-private."`
- <a id="s-1b1df3a43d"></a>`required`: `["object_path","expected_bytes","content_type","required_identity_assertions","placement_policy"]`
- <a id="s-2e642e279f"></a>`title`: `"WriteStartRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f983ec7947"></a>`content_type` | yes | type="string"; maxLength=255; minLength=1; title="Content Type" |  |
| <a id="s-ba391b3964"></a>`expected_bytes` | yes | [PositiveDecimal](#s-d52ec9da98) |  |
| <a id="s-59aac5a432"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-cd5929f979"></a>`placement_policy` | yes | type="string"; enum=["archive_default","immediate_default"]; title="Placement Policy" | Select an adapter-configured placement default. This does not establish archive membership or read readiness. |
| <a id="s-3f20a9ba4d"></a>`required_identity_assertions` | yes | type="object"; additionalProperties=(type="string"); maxProperties=64; title="Required Identity Assertions"; x-riverhog-encoded-bytes-max=16384; x-riverhog-extent={"policy":"contract_max","reason":"bounded-object-identity-assertion-envelope"} | Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions. |

### Definitions

- [PositiveDecimal](#s-d52ec9da98)

### <a id="s-d52ec9da98"></a>definition `PositiveDecimal`

- <a id="s-21a5b0cd8c"></a>`type`: `"string"`
- <a id="s-93e7de28e0"></a>`pattern`: `"^[1-9][0-9]*(?![\\s\\S])"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field content_type](#s-f983ec7947) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field object_path](#s-59aac5a432) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field required_identity_assertions](#s-3f20a9ba4d) | `encoded-size · bytes · contract_max` | maximum=16384; reason="bounded-object-identity-assertion-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field required_identity_assertions](#s-3f20a9ba4d) | `cardinality · entries · contract_max` | maximum=64; reason="bounded-object-identity-assertion-envelope" |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c976ca25f9"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-bad8db3f54"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/WriteStartRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3e3cc1f9225a12c68af5af559b58b56040d57b1af26dbc0b6ee78b657031bae9 -->

```json
{
  "$defs": {
    "PositiveDecimal": {
      "pattern": "^[1-9][0-9]*(?![\\s\\S])",
      "type": "string"
    }
  },
  "additionalProperties": false,
  "description": "Exact authority for one idempotently established nonterminal write.\n\nRepeating the same canonical request against the same configured adapter while the\nwrite remains nonterminal returns the same continuation session. Operational\ncredentials used to realize that session remain adapter-private.",
  "properties": {
    "content_type": {
      "maxLength": 255,
      "minLength": 1,
      "title": "Content Type",
      "type": "string"
    },
    "expected_bytes": {
      "$ref": "#/$defs/PositiveDecimal"
    },
    "object_path": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Object Path",
      "type": "string"
    },
    "placement_policy": {
      "description": "Select an adapter-configured placement default. This does not establish archive membership or read readiness.",
      "enum": [
        "archive_default",
        "immediate_default"
      ],
      "title": "Placement Policy",
      "type": "string"
    },
    "required_identity_assertions": {
      "additionalProperties": {
        "type": "string"
      },
      "description": "Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions.",
      "maxProperties": 64,
      "title": "Required Identity Assertions",
      "type": "object",
      "x-riverhog-encoded-bytes-max": 16384,
      "x-riverhog-extent": {
        "policy": "contract_max",
        "reason": "bounded-object-identity-assertion-envelope"
      }
    }
  },
  "required": [
    "object_path",
    "expected_bytes",
    "content_type",
    "required_identity_assertions",
    "placement_policy"
  ],
  "title": "WriteStartRequest",
  "type": "object"
}
```

</details>
