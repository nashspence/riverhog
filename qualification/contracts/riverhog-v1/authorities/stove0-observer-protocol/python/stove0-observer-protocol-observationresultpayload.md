# stove0_observer_protocol.ObservationResultPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationresultpayload:0624787402 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-51d99ad36b"></a>
| Field | Shape |
|---|---|
| <a id="s-a7ce4c833d"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-8110911b5b"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-8204a0dd3f"></a>`module` | "stove0_observer_protocol" |
| <a id="s-f1470c0c51"></a>`name` | "ObservationResultPayload" |
| <a id="s-0316a2bcdf"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.ObservationResultPayload.validate_state_payload](stove0-observer-protocol-observationresultpayload-validate-state-payload.md)
- [stove0_observer_protocol.ObservationResultPayload.canonical_subjects](stove0-observer-protocol-observationresultpayload-canonical-subjects.md)

## Governing policies

- <a id="pa-e90156a667"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationResultPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9fa1fcf899563f95fb395d4c461a782a74a448c54ab3dbd1b2bd310d93e9cef3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "1a1aa64006d73d7ae46bba02f9833cc66a623d6c3fe11d9209145c1e374810ed",
    "signature": "\"(*, format: Literal['stove0-observation-result/v1'] = 'stove0-observation-result/v1', request_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['observed', 'inapplicable', 'failed', 'canceled'], observer: stove0_protocol.models.ObserverImplementation, observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], facts_schema: stove0_protocol.models.JsonSchemaDocument | None = None, facts: dict[str, JsonValue] | None = None, facts_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, execution_evidence: dict[str, JsonValue] = <factory>, inapplicable: stove0_protocol.models.ObservationInapplicable | None = None, failure: stove0_protocol.models.ObservationFailure | None = None) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObservationResultPayload",
  "unit": "export"
}
```
