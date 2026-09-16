# stove0_core.InMemoryWorkStore.load_target_settlement_seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-load-target-428a7bd0af:56ac03cd3f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7910e0a081"></a>
- <a id="s-74ecadc841"></a>`distribution`: `stove0-server`
- <a id="s-f590466c5a"></a>`module`: `stove0_core`
- <a id="s-274f6a05c9"></a>`name`: `load_target_settlement_seal`
- <a id="s-91a36e95fa"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-383a6532d3"></a>`unit`: `member`

### Declared structure

- <a id="s-b331cc0555"></a>`kind`: `"method"`
- <a id="s-8168e09e71"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'TargetSettlementSealRecord \| None'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-55c9d5fa4a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.load_target_settlement_seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1c1991f71f750416aa9d17a2cd435a403a53d2481a789f80f00c37f31424e713 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetSettlementSealRecord | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_target_settlement_seal",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
