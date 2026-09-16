# stove0_core.InMemoryWorkStore.ensure_target_settlement_binding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-ensure-targ-a4f0490bd2:34e0dc1f9d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-69945f361f"></a>
- <a id="s-c54d468a81"></a>`distribution`: `stove0-server`
- <a id="s-8ca0656985"></a>`module`: `stove0_core`
- <a id="s-116af19409"></a>`name`: `ensure_target_settlement_binding`
- <a id="s-14282ff50d"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-5065d15c7c"></a>`unit`: `member`

### Declared structure

- <a id="s-b887fc4161"></a>`kind`: `"method"`
- <a id="s-87b04e8912"></a>`signature`: `"\"(self, record: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-d56ee969db"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.ensure_target_settlement_binding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 163d5e2c3a3b4934b271c132e56e012241953d8ea766981558cce254e6af2264 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'TargetSettlementSealRecord') -> 'TargetSettlementSealRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ensure_target_settlement_binding",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
