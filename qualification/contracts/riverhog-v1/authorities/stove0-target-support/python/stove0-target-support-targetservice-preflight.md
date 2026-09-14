# stove0_target_support.TargetService.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetservice-preflight:6207a9e3c5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-345dfdb583"></a>
- <a id="s-92fa18544a"></a>`distribution`: `stove0-target-support`
- <a id="s-9aecae8289"></a>`module`: `stove0_target_support`
- <a id="s-e5ceb5a119"></a>`name`: `preflight`
- <a id="s-617b627479"></a>`owner`: `stove0_target_support.TargetService`
- <a id="s-cc86c506f1"></a>`unit`: `member`

### Declared structure

- <a id="s-14bca8a069"></a>`kind`: `"method"`
- <a id="s-9525fe4ca5"></a>`signature`: `"\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""`

## Maintained corroboration

### Related interface records

- [TargetService](stove0-target-support-targetservice.md)

## Governing policies

- <a id="pa-d0d7c37f23"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetService.preflight`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c1c71cc9c106c3e1ce7f64adff1ced9ff2e7a2e7d0b2ae4cbf0345cb2ef82fb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "preflight",
  "owner": "stove0_target_support.TargetService",
  "unit": "member"
}
```
