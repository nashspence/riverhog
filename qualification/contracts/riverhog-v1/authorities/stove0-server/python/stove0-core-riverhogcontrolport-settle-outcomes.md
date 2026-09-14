# stove0_core.RiverhogControlPort.settle_outcomes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-settle-outcomes:321e887949 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-919ce97390"></a>
- <a id="s-f71ffbe17c"></a>`distribution`: `stove0-server`
- <a id="s-245cd5d310"></a>`module`: `stove0_core`
- <a id="s-3814af5de6"></a>`name`: `settle_outcomes`
- <a id="s-382789e64f"></a>`owner`: `stove0_core.RiverhogControlPort`
- <a id="s-938d93d150"></a>`unit`: `member`

### Declared structure

- <a id="s-5417242ce5"></a>`kind`: `"method"`
- <a id="s-ef96822d40"></a>`signature`: `"\"(self, record: 'WorkRecord', evaluation: 'BranchSetEvaluation') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-5cab52d2b9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.settle_outcomes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6997b4e8ef99a719c49406d09d3cd28691f15bc46331226b4bd6444dd5ab462b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord', evaluation: 'BranchSetEvaluation') -> 'bool'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "settle_outcomes",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```
