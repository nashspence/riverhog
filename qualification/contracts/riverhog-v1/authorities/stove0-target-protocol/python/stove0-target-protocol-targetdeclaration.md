# stove0_target_protocol.TargetDeclaration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetdeclaration:15a1d49007 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b7498f526c"></a>
| Field | Shape |
|---|---|
| <a id="s-67c81dd8b6"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-2b2ba036d5"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-ecc21766d5"></a>`module` | "stove0_target_protocol" |
| <a id="s-7fb2d4675d"></a>`name` | "TargetDeclaration" |
| <a id="s-d03c570747"></a>`unit` | "export" |

## Governing policies

- <a id="pa-996e034c1c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetDeclaration`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d27f0356a42091a6b86f7aa5defeee4f9adf25f5594a251508bcc5158d6b6e08 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "2799fc17c1e3def4ba574b3d4ac58984d664b817f1a646a63e4f35c7343b6997",
    "signature": "\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetDeclaration",
  "unit": "export"
}
```
