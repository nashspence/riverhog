# stove0_core.InMemoryWorkStore.target_disposition_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-target-disp-0c106400fd:788a68f0b0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aebc11ba0c"></a>
- <a id="s-974ad91b31"></a>`distribution`: `stove0-server`
- <a id="s-bab4f16506"></a>`module`: `stove0_core`
- <a id="s-bf5cd1ecf8"></a>`name`: `target_disposition_page`
- <a id="s-58d49baad8"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-5fc56845f9"></a>`unit`: `member`

### Declared structure

- <a id="s-2043ef45ae"></a>`kind`: `"method"`
- <a id="s-63636a8cdb"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str \| None', limit: 'int') -> 'tuple[InputDispositionDeclaration, ...]'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-082e835331"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.target_disposition_page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fe508b72e37bd2a31d1efd0c78e8ca12692c64d7f35122834bd3c85b28b25b1f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str | None', limit: 'int') -> 'tuple[InputDispositionDeclaration, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_disposition_page",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
