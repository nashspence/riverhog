# stove0_core.Stove0WorkService.begin_source_collection_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-begin-sourc-8923a8cf8d:3e3bb7e199 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-da91082ec1"></a>
- <a id="s-b5c17ad36b"></a>`distribution`: `stove0-server`
- <a id="s-8b4da2dc50"></a>`module`: `stove0_core`
- <a id="s-266ae3eeb7"></a>`name`: `begin_source_collection_retirement`
- <a id="s-0575b2dbb8"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-b6d06d727b"></a>`unit`: `member`

### Declared structure

- <a id="s-9e9b0331cc"></a>`kind`: `"method"`
- <a id="s-bf82e1ef9a"></a>`signature`: `"\"(self, work_id: 'str', collection_ids: 'Sequence[int]', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-d4a4a727fc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.begin_source_collection_retirement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 00db26f6e519007ccbc9a2ba2bc9bb5c82799aa2ace1e9d17bb065564c62e231 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', collection_ids: 'Sequence[int]', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "begin_source_collection_retirement",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
