# stove0_target_protocol.TargetOutputBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetoutputbinding:172e9723d2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4a25d10767"></a>
| Field | Shape |
|---|---|
| <a id="s-10f97efd18"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-f2e2bd557e"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-497da24f5e"></a>`module` | "stove0_target_protocol" |
| <a id="s-dd31d15fa5"></a>`name` | "TargetOutputBinding" |
| <a id="s-702c92b733"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a17bf4f544"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetOutputBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c021a406148ffe595920130bef64ae3b8067debcad3bc3378b07fb77998b1f10 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "099e02fceaedad912ee5ed40bf980f2c671acea4d76a233dc4128e1fc11f5fdb",
    "signature": "\"(*, output_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], collection: stove0_target_protocol.protocol.OutputCollectionRef, path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], media_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetOutputBinding",
  "unit": "export"
}
```
