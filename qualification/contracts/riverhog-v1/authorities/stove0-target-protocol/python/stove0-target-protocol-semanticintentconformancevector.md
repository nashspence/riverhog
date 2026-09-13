# stove0_target_protocol.SemanticIntentConformanceVector

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-semanticintentconf-7f964783b2:91733ad0b4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ec4e2f22e8"></a>
| Field | Shape |
|---|---|
| <a id="s-2be4ee5b01"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-08d23a3e57"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-6e0f6e3663"></a>`module` | "stove0_target_protocol" |
| <a id="s-89c112b446"></a>`name` | "SemanticIntentConformanceVector" |
| <a id="s-af57b88d2c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-6aa19f9ced"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.SemanticIntentConformanceVector`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e860f697719210298330b36f5122c617ba0367218d400c508819922d917a7eb5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "1675a210db6ad38f3c9e21f6aea633cfa9d851dcbaa8a1d10cac300978ad9670",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], accepted: bool, intent: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "SemanticIntentConformanceVector",
  "unit": "export"
}
```
