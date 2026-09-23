# stove0_core.InMemoryWorkStore.iter_target_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-iter-target-outputs:675f5a0736 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f1026a66fd"></a>
- <a id="s-d4430738f2"></a>`distribution`: `stove0-server`
- <a id="s-d438a56caf"></a>`module`: `stove0_core`
- <a id="s-88510d83b5"></a>`name`: `iter_target_outputs`
- <a id="s-5e4f9c558f"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-99c0a1ce82"></a>`unit`: `member`

### Declared structure

- <a id="s-61c75594ff"></a>`kind`: `"method"`
- <a id="s-f550944fed"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-094bc3498f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.iter_target_outputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e8882cb742504171b57e5c0beecc044130f23d8ea307b106e08445ef331a6bd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_outputs",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
