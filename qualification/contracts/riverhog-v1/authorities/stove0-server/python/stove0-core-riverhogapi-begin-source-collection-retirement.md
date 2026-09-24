# stove0_core.RiverhogApi.begin_source_collection_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-begin-source-coll-c7f70995b1:dfa8a0da7c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-215a6f6829"></a>
- <a id="s-a8bf619156"></a>`distribution`: `stove0-server`
- <a id="s-e13f6b481f"></a>`module`: `stove0_core`
- <a id="s-04d6d85d8f"></a>`name`: `begin_source_collection_retirement`
- <a id="s-e89bd4c5ac"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-fa75847d7f"></a>`unit`: `member`

### Declared structure

- <a id="s-5c984a1ba2"></a>`kind`: `"method"`
- <a id="s-1bd4323bd4"></a>`signature`: `"\"(self, claim_id: 'str', *, fence: 'int') -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-7a18bf65f9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.begin_source_collection_retirement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d85bef149401a4500dfe58fc4237744f99ac409a414aab76a0032753ed1fb6e6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int') -> 'ProcessingClaimDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "begin_source_collection_retirement",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
