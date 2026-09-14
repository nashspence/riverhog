# stove0_core.WorkStore.record_target_disposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-record-target-disposition:f480930ce2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-42c8acc38f"></a>
- <a id="s-a551c8402a"></a>`distribution`: `stove0-server`
- <a id="s-ded77aaa05"></a>`module`: `stove0_core`
- <a id="s-bfac0f76e6"></a>`name`: `record_target_disposition`
- <a id="s-5f3485beab"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-edb5b53824"></a>`unit`: `member`

### Declared structure

- <a id="s-72e8692f69"></a>`kind`: `"method"`
- <a id="s-c6d64fc45a"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-bba66e8011"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.record_target_disposition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4da151aa27333a156f2a02f3626f6ab66af6d8cfdf86a5faca8adc43a18ee3bf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_target_disposition",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
