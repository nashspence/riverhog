# stove0_core.WorkStore.iter_target_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-iter-target-dispositions:016a63a4d5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7fd0f5942e"></a>
- <a id="s-68ba9c12db"></a>`distribution`: `stove0-server`
- <a id="s-104c0bfbd2"></a>`module`: `stove0_core`
- <a id="s-bf135d6731"></a>`name`: `iter_target_dispositions`
- <a id="s-0df563097b"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-47919b0323"></a>`unit`: `member`

### Declared structure

- <a id="s-433256307d"></a>`kind`: `"method"`
- <a id="s-4108cda3b1"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[InputDispositionDeclaration]'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-410da13e7f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.iter_target_dispositions`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d7ef836927a33e3df35cdbbf3743130be2878473dc695fb603374752b65638a2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[InputDispositionDeclaration]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_dispositions",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
