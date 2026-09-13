# riverhog_protocol.TransformIntent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-transformintent:223a978bae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-31a07b99be"></a>
| Field | Shape |
|---|---|
| <a id="s-d5c7f60f27"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-bf4e6dfd1f"></a>`distribution` | "riverhog-protocol" |
| <a id="s-4f7f42422b"></a>`module` | "riverhog_protocol" |
| <a id="s-81e91cabf3"></a>`name` | "TransformIntent" |
| <a id="s-bcfb57ad1c"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.TransformIntent.as_dict](riverhog-protocol-transformintent-as-dict.md)
- [riverhog_protocol.TransformIntent.from_mapping](riverhog-protocol-transformintent-from-mapping.md)
- [riverhog_protocol.TransformIntent.identity_payload](riverhog-protocol-transformintent-identity-payload.md)
- [riverhog_protocol.TransformIntent.identity_sha256](riverhog-protocol-transformintent-identity-sha256.md)
- [riverhog_protocol.TransformIntent.seal](riverhog-protocol-transformintent-seal.md)
- [riverhog_protocol.TransformIntent.to_json_bytes](riverhog-protocol-transformintent-to-json-bytes.md)

## Governing policies

- <a id="pa-639faa1100"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.TransformIntent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 392df695b89099f216f4070eebd0e3a5882e3a9b2dc863e91cd8171b16b0db72 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "transform_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "recipe",
        "type": "'RecipeIdentity'"
      },
      {
        "default": "required",
        "name": "operation",
        "type": "'OperationIdentity'"
      },
      {
        "default": "required",
        "name": "inputs",
        "type": "'tuple[CollectionRootIdentity, ...]'"
      },
      {
        "default": "required",
        "name": "effective_intent",
        "type": "'dict[str, JsonValue]'"
      },
      {
        "default": "'retain'",
        "name": "retirement_policy",
        "type": "'RetirementPolicy'"
      },
      {
        "default": "0",
        "name": "retirement_grace_seconds",
        "type": "'int'"
      }
    ],
    "kind": "class",
    "signature": "\"(transform_id: 'str', recipe: 'RecipeIdentity', operation: 'OperationIdentity', inputs: 'tuple[CollectionRootIdentity, ...]', effective_intent: 'dict[str, JsonValue]', retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "TransformIntent",
  "unit": "export"
}
```
