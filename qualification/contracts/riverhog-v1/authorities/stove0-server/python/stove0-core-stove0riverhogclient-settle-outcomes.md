# stove0_core.Stove0RiverhogClient.settle_outcomes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-settle-outcomes:5c98f8676d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c49fa9a820"></a>
- <a id="s-a9a312b5e0"></a>`distribution`: `stove0-server`
- <a id="s-4464a94bb8"></a>`module`: `stove0_core`
- <a id="s-edcb445d94"></a>`name`: `settle_outcomes`
- <a id="s-ceae8f9eeb"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-19422bfffb"></a>`unit`: `member`

### Declared structure

- <a id="s-b43677000d"></a>`kind`: `"method"`
- <a id="s-63f4bad0f0"></a>`signature`: `"\"(self, record: 'WorkRecord', evaluation: 'BranchSetEvaluation') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-801114400b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.settle_outcomes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e91af46d471e49cb4dae02c08c642102ff8e8f481687d68ce68e8f7a7cf5828d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord', evaluation: 'BranchSetEvaluation') -> 'bool'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "settle_outcomes",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```
