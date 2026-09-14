# stove0_core.Stove0WorkService.request_coordination_cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-request-coo-631a35853d:4c0cb77345 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0d2c0e0120"></a>
- <a id="s-c22e6c06b3"></a>`distribution`: `stove0-server`
- <a id="s-f8550799e8"></a>`module`: `stove0_core`
- <a id="s-22cb270077"></a>`name`: `request_coordination_cancel`
- <a id="s-74ace7a299"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-7f93d9e5c6"></a>`unit`: `member`

### Declared structure

- <a id="s-2233186d97"></a>`kind`: `"method"`
- <a id="s-04800c39dd"></a>`signature`: `"\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-46f4ef6eb8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.request_coordination_cancel`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 243ddd344e36612acd328ea539ed66a4c09ec8ca9e6a23949674bb7d17af7245 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "request_coordination_cancel",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```
