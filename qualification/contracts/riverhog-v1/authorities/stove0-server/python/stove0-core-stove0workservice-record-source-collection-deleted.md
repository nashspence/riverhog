# stove0_core.Stove0WorkService.record_source_collection_deleted

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-record-sour-be29346836:2fd25d5135 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-01adfa39b4"></a>
- <a id="s-efdb5b30de"></a>`distribution`: `stove0-server`
- <a id="s-16c06e4b57"></a>`module`: `stove0_core`
- <a id="s-bac258a47e"></a>`name`: `record_source_collection_deleted`
- <a id="s-abb1ecb9df"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-1d29e08e55"></a>`unit`: `member`

### Declared structure

- <a id="s-d4ea768ed5"></a>`kind`: `"method"`
- <a id="s-04c5a281ac"></a>`signature`: `"\"(self, work_id: 'str', collection_id: 'int', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-fedb9978a7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.record_source_collection_deleted`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebe2d83467d60d21034d59d9939c295fb37a19a5c9e3277d1f7cd8e8215df78d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', collection_id: 'int', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_source_collection_deleted",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
