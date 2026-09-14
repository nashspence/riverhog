# stove0_observer_support.conformance_report

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-conformance-report:fa05b2b324 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aa3b9c912c"></a>
- <a id="s-5d8948f7f8"></a>`distribution`: `stove0-observer-support`
- <a id="s-c08a4062ac"></a>`module`: `stove0_observer_support`
- <a id="s-8e6081b310"></a>`name`: `conformance_report`
- <a id="s-081e2e52f1"></a>`unit`: `export`

### Declared structure

- <a id="s-8786d06a41"></a>`kind`: `"function"`
- <a id="s-d6bdcedf22"></a>`signature`: `"\"(client: 'ObserverClient', *, invocations: 'Sequence[ObservationInvocation]' = (), semantic_vectors: 'Sequence[SemanticFactsConformanceVectors]' = (), semantic_validators: 'SemanticValidatorProvider \| None' = None) -> 'ObserverConformanceResult'\""`

## Governing policies

- <a id="pa-4767fb39ac"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.conformance_report`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b6be68c58c7f65e85663eb8ae7160941f8ab81d7fb964d19796c5db90c1794e -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(client: 'ObserverClient', *, invocations: 'Sequence[ObservationInvocation]' = (), semantic_vectors: 'Sequence[SemanticFactsConformanceVectors]' = (), semantic_validators: 'SemanticValidatorProvider | None' = None) -> 'ObserverConformanceResult'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "conformance_report",
  "unit": "export"
}
```
