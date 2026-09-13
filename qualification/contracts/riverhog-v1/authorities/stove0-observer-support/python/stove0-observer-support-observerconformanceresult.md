# stove0_observer_support.ObserverConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observerconformanceresult:82b6a495cb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc4e720fe4"></a>
| Field | Shape |
|---|---|
| <a id="s-b37b1712e6"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-a04371e892"></a>`distribution` | "stove0-observer-support" |
| <a id="s-0b2a786915"></a>`module` | "stove0_observer_support" |
| <a id="s-ef0f9faad0"></a>`name` | "ObserverConformanceResult" |
| <a id="s-268fd9a495"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_support.ObserverConformanceResult.validate_result](stove0-observer-support-observerconformanceresult-validate-result.md)

## Governing policies

- <a id="pa-545f5f77df"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.ObserverConformanceResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1fead05c6ec6f05648d5e55e0f186946486263d9b1af2593c23ec413fc617e21 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "e27ed5abfaeb7544a3910aa4ab0216c2fc7f5fe0341296cb0b299304e601db95",
    "signature": "\"(*, format: Literal['stove0-observer-conformance-result/v1'] = 'stove0-observer-conformance-result/v1', status: Literal['conformant', 'partially-exercised', 'inspected'], descriptor: stove0_protocol.models.ObserverDescriptor, coverage: stove0_observer_support.conformance.ObserverConformanceCoverage, contracts: tuple[stove0_observer_support.conformance.ObserverContractConformance, ...]) -> None\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "ObserverConformanceResult",
  "unit": "export"
}
```
