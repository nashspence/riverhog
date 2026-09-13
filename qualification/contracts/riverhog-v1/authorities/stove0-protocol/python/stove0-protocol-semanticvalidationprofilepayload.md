# stove0_protocol.SemanticValidationProfilePayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-semanticvalidationprofilepayload:728cbc385f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-27f4fad135"></a>
| Field | Shape |
|---|---|
| <a id="s-44af0f1047"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-6628a8c16f"></a>`distribution` | "stove0-protocol" |
| <a id="s-dbe38b52eb"></a>`module` | "stove0_protocol" |
| <a id="s-2c4e5f47ba"></a>`name` | "SemanticValidationProfilePayload" |
| <a id="s-ed40a678a8"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.SemanticValidationProfilePayload.canonical_rules](stove0-protocol-semanticvalidationprofilepayload-canonical-rules.md)

## Governing policies

- <a id="pa-e3e7911854"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.SemanticValidationProfilePayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1edf3fea1251683a35d4d9e86df419916bc6abd788d0c31797c82b51d3b4fcec -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "9feba86c6b0478a980740aa7d15a0534fd499479fc164ec3f3b3f3b72b5182f9",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], rules: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)], conformance_vectors_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "SemanticValidationProfilePayload",
  "unit": "export"
}
```
