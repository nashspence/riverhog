# stove0_core.Stove0WorkService.complete_abandon

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-complete-abandon:db37c8132e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-11d477d3b5"></a>
- <a id="s-e89ae9b2bd"></a>`distribution`: `stove0-server`
- <a id="s-d024db338e"></a>`module`: `stove0_core`
- <a id="s-94ab985337"></a>`name`: `complete_abandon`
- <a id="s-3f163938bd"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-f9048885e5"></a>`unit`: `member`

### Declared structure

- <a id="s-30a6c400ec"></a>`kind`: `"method"`
- <a id="s-afc1e7cdc9"></a>`signature`: `"\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-539bf2e736"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.complete_abandon`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 17ee2af9dff035673804ccc3bffbb8bae7cfc4c92880515fda6f9f441c8dfeff -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "complete_abandon",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
