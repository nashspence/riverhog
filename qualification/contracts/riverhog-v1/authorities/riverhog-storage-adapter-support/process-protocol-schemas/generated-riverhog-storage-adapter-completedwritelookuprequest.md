# generated:riverhog-storage-adapter: CompletedWriteLookupRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol-schemas:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-comple-0fb020c5d9:d8b7289b65 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol Schemas](index.md) |

## External contract

<a id="s-95c307eef1"></a>

- <a id="s-37f9a223b9"></a>`type`: `"object"`
- <a id="s-b58067e30e"></a>`additionalProperties`: `false`
- <a id="s-41b6488a2d"></a>`required`: `["object_path","expected_bytes","expected_content_type","required_identity_assertions","expected_placement"]`
- <a id="s-03703c29e8"></a>`title`: `"CompletedWriteLookupRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d6b613d625"></a>`expected_bytes` | yes | [PositiveDecimal](#s-53abf440a5) |  |
| <a id="s-558690d0bf"></a>`expected_content_type` | yes | type="string"; maxLength=255; minLength=1; title="Expected Content Type" |  |
| <a id="s-8c0b60131f"></a>`expected_placement` | yes | type="string"; enum=["archive","immediate"]; title="Expected Placement" |  |
| <a id="s-ceec1bb3a7"></a>`object_path` | yes | type="string"; maxLength=4096; minLength=1; title="Object Path" |  |
| <a id="s-dc8a372d2a"></a>`required_identity_assertions` | yes | type="object"; additionalProperties=(type="string"); maxProperties=64; title="Required Identity Assertions"; x-riverhog-encoded-bytes-max=16384; x-riverhog-extent={"policy":"contract_max","reason":"bounded-object-identity-assertion-envelope"} | Inert caller-owned facts used only to identify and reconcile an exact stored object. Adapters canonicalize, persist, return, and compare these assertions; they must not interpret them as routing, retrieval, retention, credentials, placement, or provider-control instructions. Adapters may retain additional adapter-private assertions. |

### Definitions

- [PositiveDecimal](#s-53abf440a5)

### <a id="s-53abf440a5"></a>definition `PositiveDecimal`

- <a id="s-26846a936f"></a>`type`: `"string"`
- <a id="s-1e72974aa8"></a>`pattern`: `"^[1-9][0-9]*(?![\\s\\S])"`

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field expected_content_type](#s-558690d0bf) | `length · characters · contract_max` | maximum=255; minimum=1; reason="schema-maximum" |
| [field object_path](#s-ceec1bb3a7) | `length · characters · contract_max` | maximum=4096; minimum=1; reason="schema-maximum" |
| [field required_identity_assertions](#s-dc8a372d2a) | `encoded-size · bytes · contract_max` | maximum=16384; reason="bounded-object-identity-assertion-envelope"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |
| [field required_identity_assertions](#s-dc8a372d2a) | `cardinality · entries · contract_max` | maximum=64; reason="bounded-object-identity-assertion-envelope" |

## Maintained corroboration

### Related interface records

- [generated:riverhog-storage-adapter protocol](../process-protocol/generated-riverhog-storage-adapter-protocol.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-35355dd850"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-98dada603a"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources/authorities.md#src-ef281f2471) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/schemas.py::storage\_adapter\_schema\_bundle](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py)

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/schemas/CompletedWriteLookupRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8557b5a8676d11e86387a84720802c031aad1a451b29d1b96872109b8a993834 -->

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
      "$ref": "#/$defs/PositiveDecimal"
    },
    "expected_content_type": {
      "maxLength": 255,
      "minLength": 1,
      "title": "Expected Content Type",
      "type": "string"
    },
    "expected_placement": {
      "enum": [
        "archive",
        "immediate"
      ],
      "title": "Expected Placement",
      "type": "string"
    },
    "object_path": {
      "maxLength": 4096,
      "minLength": 1,
      "title": "Object Path",
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
    "expected_content_type",
    "required_identity_assertions",
    "expected_placement"
  ],
  "title": "CompletedWriteLookupRequest",
  "type": "object"
}
```

</details>
