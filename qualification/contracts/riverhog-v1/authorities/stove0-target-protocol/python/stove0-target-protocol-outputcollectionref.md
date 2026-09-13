# stove0_target_protocol.OutputCollectionRef

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-outputcollectionref:6abe430d46 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-59a993db4f"></a>
| Field | Shape |
|---|---|
| <a id="s-ae75453dc4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-0b3e486105"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-fe6ce97bc4"></a>`module` | "stove0_target_protocol" |
| <a id="s-5e102fd311"></a>`name` | "OutputCollectionRef" |
| <a id="s-6eb194f0bf"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3743872219"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OutputCollectionRef`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 696b0a5ead5f5488cbd05c01a8eafce5a8011d9c5fc892e9d68607f0d0473bf1 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5e114f49bd35594b81763b7a7bd469021694265597f256e3648d44f6284201e1",
    "signature": "\"(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "OutputCollectionRef",
  "unit": "export"
}
```
