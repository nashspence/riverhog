# stove0_target_protocol.TargetOperationSupport

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetoperationsupport:b53f051474 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ece49ed72b"></a>
| Field | Shape |
|---|---|
| <a id="s-d82aec2e2e"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-fb8dec99b2"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-d3dd75b48f"></a>`module` | "stove0_target_protocol" |
| <a id="s-b261ef37af"></a>`name` | "TargetOperationSupport" |
| <a id="s-058a6a2cb6"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a56400b1e3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetOperationSupport`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 266b876abdf41ab75e7d6860c61a7307828c8dc691a070c18c741af204aec85d -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "6d124a4d31728580104c9df9fec11f2f903e65acc80d328b6449bf5ac33d4def",
    "signature": "\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', options_schema: stove0_protocol.models.JsonSchemaDocument) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetOperationSupport",
  "unit": "export"
}
```
