# stove0_target_support.conformance_report

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-conformance-report:d11c6d861d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b349b8e722"></a>
- <a id="s-a48ff96315"></a>`distribution`: `stove0-target-support`
- <a id="s-cfcef4a43f"></a>`module`: `stove0_target_support`
- <a id="s-b0efcdb890"></a>`name`: `conformance_report`
- <a id="s-8443e83a48"></a>`unit`: `export`

### Declared structure

- <a id="s-4f35a7d814"></a>`kind`: `"function"`
- <a id="s-9c1352ea20"></a>`signature`: `"\"(client: 'TargetClient', *, cases: 'Sequence[TargetConformanceCase]' = ()) -> 'TargetConformanceResult'\""`

## Governing policies

- <a id="pa-75bd6e8ad5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.conformance_report`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8207df32bd349574846c555a028ce939b4684d10bd6faf4b6f2b711c088a8191 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(client: 'TargetClient', *, cases: 'Sequence[TargetConformanceCase]' = ()) -> 'TargetConformanceResult'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "conformance_report",
  "unit": "export"
}
```
