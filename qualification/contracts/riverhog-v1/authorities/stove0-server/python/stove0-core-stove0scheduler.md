# stove0_core.Stove0Scheduler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0scheduler:2c8abc0818 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e32802d7cf"></a>
- <a id="s-0c14982fe8"></a>`distribution`: `stove0-server`
- <a id="s-ec0fd4d5b3"></a>`module`: `stove0_core`
- <a id="s-ed7ad16265"></a>`name`: `Stove0Scheduler`
- <a id="s-3e3b76b538"></a>`unit`: `export`

### Declared structure

- <a id="s-d84283fa69"></a>`kind`: `"class"`
- <a id="s-65f50f6d1c"></a>`signature`: `"\"(*, coordinator: 'Stove0Coordinator', state: 'SqlAlchemyStateStore', production_seals: 'ProductionSealProcessor \| None' = None, admission: 'AdmissionProcessor \| None' = None, operational_state_retention_seconds: 'int' = 2592000) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [advance](stove0-core-stove0scheduler-advance.md)
- [run_once](stove0-core-stove0scheduler-run-once.md)

## Governing policies

- <a id="pa-6e40bfe717"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0Scheduler`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d30e65e43f05a5208516df5e21a53eeaa6def5a7dabbec808c01093fd04d9720 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, coordinator: 'Stove0Coordinator', state: 'SqlAlchemyStateStore', production_seals: 'ProductionSealProcessor | None' = None, admission: 'AdmissionProcessor | None' = None, operational_state_retention_seconds: 'int' = 2592000) -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "Stove0Scheduler",
  "unit": "export"
}
```

</details>
