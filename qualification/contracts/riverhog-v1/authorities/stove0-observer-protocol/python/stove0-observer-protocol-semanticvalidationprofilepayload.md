# stove0_observer_protocol.SemanticValidationProfilePayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticvalidati-8cc1ee2d4c:fc3aaf3049 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-791e5d9344"></a>
| Field | Shape |
|---|---|
| <a id="s-bfb093db7c"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-374eb46c41"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-3cdb8f89fc"></a>`module` | "stove0_observer_protocol" |
| <a id="s-82e2af3a58"></a>`name` | "SemanticValidationProfilePayload" |
| <a id="s-0434b6e6e0"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.SemanticValidationProfilePayload.canonical_rules](stove0-observer-protocol-semanticvalidationprofilepayload-canonical-rules.md)

## Governing policies

- <a id="pa-770ef4c6dd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticValidationProfilePayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9615f74449d645b32f560377b87c9bb2567e4a096bb12d0e79e50534b8ef3796 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "9feba86c6b0478a980740aa7d15a0534fd499479fc164ec3f3b3f3b72b5182f9",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], rules: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)], conformance_vectors_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "SemanticValidationProfilePayload",
  "unit": "export"
}
```
