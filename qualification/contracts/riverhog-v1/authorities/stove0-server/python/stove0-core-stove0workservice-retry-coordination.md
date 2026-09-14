# stove0_core.Stove0WorkService.retry_coordination

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-retry-coordination:6e346aec2d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1dd4a7d094"></a>
- <a id="s-57a0011409"></a>`distribution`: `stove0-server`
- <a id="s-ab1bc92ce1"></a>`module`: `stove0_core`
- <a id="s-a5c871bdb5"></a>`name`: `retry_coordination`
- <a id="s-f8519d2a6f"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-3513301d29"></a>`unit`: `member`

### Declared structure

- <a id="s-1dd9487e3c"></a>`kind`: `"method"`
- <a id="s-fb5c59ee2c"></a>`signature`: `"\"(self, work_id: 'str', *, claim_id: 'str', fence: 'int', expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-34f42ae487"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.retry_coordination`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d7b749e626ae932c9e21e450f31f6ae731ac0399d5bf6f6580a49262080efb4b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, claim_id: 'str', fence: 'int', expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "retry_coordination",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```
