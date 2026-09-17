# stove0_core.InMemoryWorkStore.create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-create:9603e332f9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-28fb0a3462"></a>
- <a id="s-281f2c4706"></a>`distribution`: `stove0-server`
- <a id="s-aadd72a2cd"></a>`module`: `stove0_core`
- <a id="s-88b3611092"></a>`name`: `create`
- <a id="s-38547dc3b2"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-1e1a11340e"></a>`unit`: `member`

### Declared structure

- <a id="s-c29b188d9a"></a>`kind`: `"method"`
- <a id="s-08d6d9990d"></a>`signature`: `"\"(self, record: 'WorkRecord') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-35440a9747"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.create`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12ae54e0818e69bd18c3b6d0c61518cc7312c918500bd2ff1056930c5ee85c9a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "create",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
