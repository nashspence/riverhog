# stove0_observer_protocol.ObservationInvocation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationinvocation:665ac541d4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0a9c0d3b70"></a>
| Field | Shape |
|---|---|
| <a id="s-994e814fca"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-49ec3511db"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-bb267f858b"></a>`module` | "stove0_observer_protocol" |
| <a id="s-a283eb5adf"></a>`name` | "ObservationInvocation" |
| <a id="s-c66a337eab"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.ObservationInvocation.canonical_claim_id](stove0-observer-protocol-observationinvocation-canonical-claim-id.md)

## Governing policies

- <a id="pa-5c68c4d5d8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationInvocation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a3b9556c0d1939b33bdcf300f6d2e9ea73e1bcea5db2a1f81bdbd3164860c257 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "82cb342e57cc6456dc2d8acdb09d8836f7be2d23c0d8d5ab5ce8d23a5d8c32d7",
    "signature": "'(*, request: stove0_protocol.models.ObservationRequest, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], runtime: stove0_protocol.models.ObserverRuntimeAuthority) -> None'"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObservationInvocation",
  "unit": "export"
}
```
