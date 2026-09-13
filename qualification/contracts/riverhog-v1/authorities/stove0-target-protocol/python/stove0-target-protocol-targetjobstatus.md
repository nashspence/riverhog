# stove0_target_protocol.TargetJobStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetjobstatus:dc067334d4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-31cb463d3b"></a>
| Field | Shape |
|---|---|
| <a id="s-0592567689"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-48531cee14"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-0ba1fd98bc"></a>`module` | "stove0_target_protocol" |
| <a id="s-ed20c46453"></a>`name` | "TargetJobStatus" |
| <a id="s-4aa842cf1e"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetJobStatus.canonical_derivation](stove0-target-protocol-targetjobstatus-canonical-derivation.md)
- [stove0_target_protocol.TargetJobStatus.validate_terminal_shape](stove0-target-protocol-targetjobstatus-validate-terminal-shape.md)

## Governing policies

- <a id="pa-aa7780be93"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetJobStatus`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a22d31c2fac91aced510fe813326719b9daa5247eefa272ec4e85972ad9a4759 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "c29a6125c1975f597c8de13fed65f3aa4e4286f4616fa74161074c7217e67684",
    "signature": "\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['queued', 'running', 'canceling', 'interrupted', 'inapplicable', 'succeeded', 'failed', 'canceled'], attempt: Annotated[int, Ge(ge=1)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], progress: stove0_target_protocol.protocol.TargetProgress, production: stove0_target_protocol.protocol.TargetProductionAuthority | None = None, output_collection: stove0_target_protocol.protocol.OutputCollectionRef | None = None, execution_evidence: stove0_target_protocol.protocol.TargetExecutionEvidence | None = None, derivation: dict[str, typing.Any] | None = None, effect_receipt: stove0_target_protocol.protocol.ExternalEffectReceipt | None = None, failure: stove0_target_protocol.protocol.TargetFailure | None = None, inapplicable: stove0_target_protocol.protocol.TargetInapplicable | None = None) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetJobStatus",
  "unit": "export"
}
```
