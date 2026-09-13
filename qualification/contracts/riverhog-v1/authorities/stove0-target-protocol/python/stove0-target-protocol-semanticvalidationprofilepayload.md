# stove0_target_protocol.SemanticValidationProfilePayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-semanticvalidation-359c633a35:46fd5556f6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5caabc007e"></a>
| Field | Shape |
|---|---|
| <a id="s-02c2c0d0ca"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-3e9871fa7f"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-fac16c3375"></a>`module` | "stove0_target_protocol" |
| <a id="s-0d66396c1b"></a>`name` | "SemanticValidationProfilePayload" |
| <a id="s-2aac0ba078"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.SemanticValidationProfilePayload.canonical_rules](stove0-target-protocol-semanticvalidationprofilepayload-canonical-rules.md)

## Governing policies

- <a id="pa-2aaed38687"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.SemanticValidationProfilePayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 179545f8502a29f75b690178a76465410023fb4bf552dfcf111c592277651120 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "9feba86c6b0478a980740aa7d15a0534fd499479fc164ec3f3b3f3b72b5182f9",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], rules: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)], conformance_vectors_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "SemanticValidationProfilePayload",
  "unit": "export"
}
```
