# stove0_core.Stove0WorkService.mark_inapplicable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-mark-inapplicable:3cfb1a61a9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7e79632f34"></a>
- <a id="s-c6b52373d2"></a>`distribution`: `stove0-server`
- <a id="s-8e781ddabd"></a>`module`: `stove0_core`
- <a id="s-c84703b921"></a>`name`: `mark_inapplicable`
- <a id="s-1af4be27b5"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-286667c808"></a>`unit`: `member`

### Declared structure

- <a id="s-f042fbf026"></a>`kind`: `"method"`
- <a id="s-6bf3ca3b57"></a>`signature`: `"\"(self, work_id: 'str', outcome: 'WorkInapplicable', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-8a56028bc0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.mark_inapplicable`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2151748b58d9319e6930288fc2c8552370c128df10feefbe296896919eebe0f1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', outcome: 'WorkInapplicable', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "mark_inapplicable",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
