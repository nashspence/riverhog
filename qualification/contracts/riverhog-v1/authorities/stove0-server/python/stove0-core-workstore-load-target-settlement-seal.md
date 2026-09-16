# stove0_core.WorkStore.load_target_settlement_seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-load-target-settlement-seal:38e9241170 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-042f926504"></a>
- <a id="s-07abc6edaf"></a>`distribution`: `stove0-server`
- <a id="s-a285709786"></a>`module`: `stove0_core`
- <a id="s-84be94efa5"></a>`name`: `load_target_settlement_seal`
- <a id="s-69287a983d"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-b865bd7565"></a>`unit`: `member`

### Declared structure

- <a id="s-baf3302e36"></a>`kind`: `"method"`
- <a id="s-66fe12b0c1"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'TargetSettlementSealRecord \| None'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-935b2f9b1b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.load_target_settlement_seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: faa7aca876660b24bc4d8911dd64a87e2fb2c61a8cc06e805198a5b24e7197d8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetSettlementSealRecord | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_target_settlement_seal",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
