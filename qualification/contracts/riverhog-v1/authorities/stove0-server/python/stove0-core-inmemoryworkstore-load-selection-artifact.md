# stove0_core.InMemoryWorkStore.load_selection_artifact

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-load-select-be10540ec8:cab853e8ad -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-005ee4f7f9"></a>
- <a id="s-d61ca69f0c"></a>`distribution`: `stove0-server`
- <a id="s-c3fe02e9ad"></a>`module`: `stove0_core`
- <a id="s-572bd3b27e"></a>`name`: `load_selection_artifact`
- <a id="s-f828d9b058"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-d1b930025f"></a>`unit`: `member`

### Declared structure

- <a id="s-658b7b0df2"></a>`kind`: `"method"`
- <a id="s-4640ccada4"></a>`signature`: `"\"(self, selection_sha256: 'str', artifact_id: 'str') -> 'WorkArtifactSubject \| None'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-c1b88f3de4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.load_selection_artifact`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 451226f16310e2c6b12891f2a4fb55f09650b71c6d23b0751378fb638df82d84 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str', artifact_id: 'str') -> 'WorkArtifactSubject | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_selection_artifact",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
