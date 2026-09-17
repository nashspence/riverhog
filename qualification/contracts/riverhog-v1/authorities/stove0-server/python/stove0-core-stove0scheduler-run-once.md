# stove0_core.Stove0Scheduler.run_once

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0scheduler-run-once:7391cf644f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c10b605328"></a>
- <a id="s-88f6147166"></a>`distribution`: `stove0-server`
- <a id="s-885f39d5df"></a>`module`: `stove0_core`
- <a id="s-8dfa108bb2"></a>`name`: `run_once`
- <a id="s-4c456b990c"></a>`owner`: `stove0_core.Stove0Scheduler`
- <a id="s-8f488ba3fa"></a>`unit`: `member`

### Declared structure

- <a id="s-8ae46594be"></a>`kind`: `"method"`
- <a id="s-966696a2e3"></a>`signature`: `"\"(self, *, role: 'SchedulerRole' = 'combined', work_limit: 'int' = 25) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [Stove0Scheduler](stove0-core-stove0scheduler.md)

## Governing policies

- <a id="pa-fc04699eef"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0Scheduler.run_once`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f1ffd844a1bb6c1e6310a75302d5cbd227ec895b08ae8c0e73d22429af75eb9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, role: 'SchedulerRole' = 'combined', work_limit: 'int' = 25) -> 'dict[str, object]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "run_once",
  "owner": "stove0_core.Stove0Scheduler",
  "unit": "member"
}
```

</details>
