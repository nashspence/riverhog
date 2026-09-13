# stove0_review_planning.ReviewVariant

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-planning:stove0-review-planning-reviewvariant:6be3dc46a8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-planning](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1350e31c69"></a>
| Field | Shape |
|---|---|
| <a id="s-d6ad71f5fd"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-ad57446dae"></a>`distribution` | "stove0-review-planning" |
| <a id="s-2ce265330e"></a>`module` | "stove0_review_planning" |
| <a id="s-7e7ebb9ff8"></a>`name` | "ReviewVariant" |
| <a id="s-6f254cb7fe"></a>`unit` | "export" |

## Governing policies

- <a id="pa-ebd2e1f7cd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-planning:stove0_review_planning](../../../evidence/sources.md#src-354ae519e9) — `reference/stove0/targets/review/planning/src/stove0_review_planning/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_planning.ReviewVariant`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 538cccd6f1ecd7f7a443cf06f09cc8129ca41996cf0a77305f5ca6395d81c08c -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "4d9480938840f87d7b1881952c04480a5d339ff1a809971d9445974dcc07287b",
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,158}[a-z0-9])?$')], portable_intent: dict[str, JsonValue] = <factory>, target_options: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "stove0-review-planning",
  "module": "stove0_review_planning",
  "name": "ReviewVariant",
  "unit": "export"
}
```
