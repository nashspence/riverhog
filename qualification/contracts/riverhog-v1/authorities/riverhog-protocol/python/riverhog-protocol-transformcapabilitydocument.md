# riverhog_protocol.TransformCapabilityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-transformcapabilitydocument:90d277fbc4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-80113a1f97"></a>
| Field | Shape |
|---|---|
| <a id="s-da6b072332"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-7236ba0473"></a>`distribution` | "riverhog-protocol" |
| <a id="s-03fd69d195"></a>`module` | "riverhog_protocol" |
| <a id="s-e9531ea968"></a>`name` | "TransformCapabilityDocument" |
| <a id="s-8442e14233"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.TransformCapabilityDocument.validate_capability](riverhog-protocol-transformcapabilitydocument-validate-capability.md)

## Governing policies

- <a id="pa-eead9e1ca8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.TransformCapabilityDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dce23ee865705f40e169cc037d521f4ff6cae08882da66801371a8a36ffec5c4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "ab68b2b6cafdde0f1141de348d61664c5c4b32b4c5589b70902e242cf3600af1",
    "signature": "\"(*, format: Literal['riverhog-transform-capability/v1'], id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], claim_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], fence: Annotated[int, Ge(ge=1)], audience: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9][a-z0-9._:/-]{0,299}$')], actions: Annotated[list[Literal['read-inputs', 'write-output']], MinLen(min_length=1)], state: Literal['receiving', 'active'], principal_app: Annotated[str, MinLen(min_length=1), MaxLen(max_length=300)], expires_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=64)], artifacts: riverhog_protocol.collection_workflow_transport.ArtifactReceivingSetDocument, token: Annotated[str, _PydanticGeneralMetadata(pattern='^rhc_[A-Za-z0-9_-]+$')]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "TransformCapabilityDocument",
  "unit": "export"
}
```
