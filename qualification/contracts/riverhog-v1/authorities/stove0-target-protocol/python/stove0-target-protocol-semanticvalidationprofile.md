# stove0_target_protocol.SemanticValidationProfile

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-semanticvalidationprofile:e1e049709e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a2307d99c8"></a>
| Field | Shape |
|---|---|
| <a id="s-0fba4d5cad"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-d62501b350"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-cdd9a09c82"></a>`module` | "stove0_target_protocol" |
| <a id="s-7ce2daabed"></a>`name` | "SemanticValidationProfile" |
| <a id="s-d7ed8879c7"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.SemanticValidationProfile.verify_digest](stove0-target-protocol-semanticvalidationprofile-verify-digest.md)
- [stove0_target_protocol.SemanticValidationProfile.seal](stove0-target-protocol-semanticvalidationprofile-seal.md)

## Governing policies

- <a id="pa-fa1680cb87"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.SemanticValidationProfile`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39e7173e80f43eb0e6a08c5cf8bc091b5832eaed686756648cf9d91d4acd9975 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6b8688e96bcf912f0eab842d27ff10cea7bc3bb1b53f4b5510968ba90d9d1c9b",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], rules: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)], conformance_vectors_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, profile_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "SemanticValidationProfile",
  "unit": "export"
}
```
