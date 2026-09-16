# stove0_core.Stove0Coordinator.step

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0coordinator-step:468d48401d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-171c8cb08c"></a>
- <a id="s-c988cffe8b"></a>`distribution`: `stove0-server`
- <a id="s-6240bd7395"></a>`module`: `stove0_core`
- <a id="s-95897c63aa"></a>`name`: `step`
- <a id="s-1116047b71"></a>`owner`: `stove0_core.Stove0Coordinator`
- <a id="s-0acd0d30f3"></a>`unit`: `member`

### Declared structure

- <a id="s-7ec092437c"></a>`kind`: `"method"`
- <a id="s-690df90050"></a>`signature`: `"\"(self, work_id: 'str') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0Coordinator](stove0-core-stove0coordinator.md)

## Governing policies

- <a id="pa-7ec75fa689"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0Coordinator.step`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e1d28e874b0719feeea2908ac1bc9dc8d7ada4771e652fdf9ad0a7bebb1759dc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "step",
  "owner": "stove0_core.Stove0Coordinator",
  "unit": "member"
}
```

</details>
