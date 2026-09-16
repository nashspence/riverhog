# stove0_core.InMemoryWorkStore.record_target_disposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-record-targ-6a027aeb0d:4fcfe1e7b2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2d50337bf6"></a>
- <a id="s-d82d1e6ec9"></a>`distribution`: `stove0-server`
- <a id="s-ec40f9746d"></a>`module`: `stove0_core`
- <a id="s-826fbf82a0"></a>`name`: `record_target_disposition`
- <a id="s-5dcc911239"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-66b285a419"></a>`unit`: `member`

### Declared structure

- <a id="s-bd12509043"></a>`kind`: `"method"`
- <a id="s-b5f2e4a68e"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-dda54bf0ff"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.record_target_disposition`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4df30e17d7bbcafa1a80a1a3e5bb6f2d992ee7de741f05dd500a99f8c47174e2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', disposition: 'InputDispositionDeclaration') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_target_disposition",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
