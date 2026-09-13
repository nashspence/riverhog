# stove0_target_protocol.TargetPreflightRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetpreflightrequest:ed9797936f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f3b6918081"></a>
| Field | Shape |
|---|---|
| <a id="s-d2c9e6dfc4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-3f980450f2"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-1835afa490"></a>`module` | "stove0_target_protocol" |
| <a id="s-7274214529"></a>`name` | "TargetPreflightRequest" |
| <a id="s-40f58e04fd"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetPreflightRequest.canonical_observations](stove0-target-protocol-targetpreflightrequest-canonical-observations.md)

## Governing policies

- <a id="pa-235ed44fd9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetPreflightRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d1dfb2f42488717eb6bf571617a3bffd8e580ce70507c6fe214f94f0d9753be -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "c8edfde8d715a2d94c2d913d1e5e0d71a5b583830587bb7712098fa3b1ce301f",
    "signature": "\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = ()) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetPreflightRequest",
  "unit": "export"
}
```
