# stove0_target_support.OutputCollectionRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-outputcollectionref:0b4f65552f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2ab99788b2"></a>
| Field | Shape |
|---|---|
| <a id="s-06ecb63616"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-676fa2817a"></a>`distribution` | "stove0-target-support" |
| <a id="s-bfe0b492c0"></a>`module` | "stove0_target_support" |
| <a id="s-6e28c055ac"></a>`name` | "OutputCollectionRef" |
| <a id="s-f9218583ad"></a>`unit` | "export" |

## Governing policies

- <a id="pa-1b710b876e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.OutputCollectionRef`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7dd8c86e19e30d7e3d7397107f2e6e7a94d5e9873930e81d429dc9976563b744 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5e114f49bd35594b81763b7a7bd469021694265597f256e3648d44f6284201e1",
    "signature": "\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "OutputCollectionRef",
  "unit": "export"
}
```
