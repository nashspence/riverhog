# stove0_target_support.PersistentTargetService.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-persistenttargetser-77edd738ff:d41fe42d3b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-643489b6a8"></a>
- <a id="s-8afb2f34e8"></a>`distribution`: `stove0-target-support`
- <a id="s-1322246ca6"></a>`module`: `stove0_target_support`
- <a id="s-91ace3cab0"></a>`name`: `preflight`
- <a id="s-bc5a5954e7"></a>`owner`: `stove0_target_support.PersistentTargetService`
- <a id="s-6f2af90aca"></a>`unit`: `member`

### Declared structure

- <a id="s-07c87d06a2"></a>`kind`: `"method"`
- <a id="s-388cfdd79c"></a>`signature`: `"\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""`

## Maintained corroboration

### Related interface records

- [PersistentTargetService](stove0-target-support-persistenttargetservice.md)

## Governing policies

- <a id="pa-c6c4d2f999"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.PersistentTargetService.preflight`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6c88da7d40c180d250d71d48e550e883d7234989ebe5567498cb6bd0614e0498 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "preflight",
  "owner": "stove0_target_support.PersistentTargetService",
  "unit": "member"
}
```
