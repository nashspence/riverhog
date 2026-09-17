# stove0_core.SqlAlchemyStateStore.ensure_target_production_receiving

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-ensure-t-d776ecf56a:3ec360896c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-57058a68e1"></a>
- <a id="s-b55e5eff2d"></a>`distribution`: `stove0-server`
- <a id="s-daf5d29e21"></a>`module`: `stove0_core`
- <a id="s-2d103e1200"></a>`name`: `ensure_target_production_receiving`
- <a id="s-69b375d14b"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-fd01abf7f7"></a>`unit`: `member`

### Declared structure

- <a id="s-e00b2f7c6f"></a>`kind`: `"method"`
- <a id="s-e85771dab3"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-ee8a37d294"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.ensure_target_production_receiving`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c87f5535ae325064de00459c6af934e08d5a3cbc374f63ec4cf32e6fe9dfc5bd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ensure_target_production_receiving",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
