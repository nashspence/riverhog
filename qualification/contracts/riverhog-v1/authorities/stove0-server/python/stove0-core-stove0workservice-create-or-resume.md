# stove0_core.Stove0WorkService.create_or_resume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-create-or-resume:b9199b93a1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bbd003c719"></a>
- <a id="s-8a86786515"></a>`distribution`: `stove0-server`
- <a id="s-541af25f32"></a>`module`: `stove0_core`
- <a id="s-379d3133b3"></a>`name`: `create_or_resume`
- <a id="s-9f841c9a8b"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-9525623b20"></a>`unit`: `member`

### Declared structure

- <a id="s-b670a0fbe1"></a>`kind`: `"method"`
- <a id="s-4e13f7ff8f"></a>`signature`: `"\"(self, work: 'WorkIdentity', *, preview: 'WorkflowPreview \| None' = None) -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-208312377e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.create_or_resume`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4bfdbe119e039711103e67d07ca1dad1d0657670f302b6648064f53e1c8e6ffa -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work: 'WorkIdentity', *, preview: 'WorkflowPreview | None' = None) -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "create_or_resume",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
