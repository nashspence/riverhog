# stove0_target_support.TargetConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetconformanceresult:6a239f5efb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-50258ec12f"></a>
| Field | Shape |
|---|---|
| <a id="s-c594642500"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-00ad0d6a6e"></a>`distribution` | "stove0-target-support" |
| <a id="s-bbc1b58eb4"></a>`module` | "stove0_target_support" |
| <a id="s-7fb0038d8f"></a>`name` | "TargetConformanceResult" |
| <a id="s-85f5f66d61"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetConformanceResult.validate_result](stove0-target-support-targetconformanceresult-validate-result.md)

## Governing policies

- <a id="pa-9ffbbb9306"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetConformanceResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 225a810b0b5adb2ef5169f386c9c258cbdb416497aa59018b4bbe3245dfe7514 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "dacd9898c46672af35bfeb83fabcf79bcb811edbef134d19b75e3c4a7f66b543",
    "signature": "\"(*, format: Literal['stove0-target-conformance-result/v1'] = 'stove0-target-conformance-result/v1', status: Literal['conformant', 'partially-exercised', 'inspected'], target: stove0_target_protocol.protocol.TargetContract, coverage: stove0_target_support.conformance.TargetConformanceCoverage, operations: tuple[stove0_target_support.conformance.TargetOperationConformance, ...], operation_evidence: tuple[stove0_target_support.conformance.TargetOperationConformanceEvidence, ...] = ()) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetConformanceResult",
  "unit": "export"
}
```
