# stove0_core.Stove0WorkService.cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-cancel:113a35639e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6a6ffc580e"></a>
- <a id="s-766592f3c5"></a>`distribution`: `stove0-server`
- <a id="s-1965926c3b"></a>`module`: `stove0_core`
- <a id="s-99e0ce33d3"></a>`name`: `cancel`
- <a id="s-0efa3fb007"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-80e3cbaba4"></a>`unit`: `member`

### Declared structure

- <a id="s-8c2eb78c32"></a>`kind`: `"method"`
- <a id="s-89b34cbb98"></a>`signature`: `"\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-42ab88d2a9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.cancel`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0378357c6713b0e424beab6810d470d9830474cd635912bfb89be44b8b43a0aa -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "cancel",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
