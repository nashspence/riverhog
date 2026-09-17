# stove0_core.Stove0WorkService.retry_failed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-retry-failed:bf513bdd70 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-65b73372bf"></a>
- <a id="s-48e81eaf5f"></a>`distribution`: `stove0-server`
- <a id="s-2715ea80b8"></a>`module`: `stove0_core`
- <a id="s-632cbda5c5"></a>`name`: `retry_failed`
- <a id="s-45ad4208e6"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-5febc7ac67"></a>`unit`: `member`

### Declared structure

- <a id="s-69b953634f"></a>`kind`: `"method"`
- <a id="s-e67800f239"></a>`signature`: `"\"(self, work_id: 'str', *, claim_id: 'str', fence: 'int', expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-b2b77551be"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.retry_failed`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6f8f0372df805254dfe5b5eff1497d21ee15ecfc17f2c1cea6f71f034d085c1a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, claim_id: 'str', fence: 'int', expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "retry_failed",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
