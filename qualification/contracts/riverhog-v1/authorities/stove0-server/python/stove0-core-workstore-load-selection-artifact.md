# stove0_core.WorkStore.load_selection_artifact

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-load-selection-artifact:94c84ce011 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5492720673"></a>
- <a id="s-a5f85fe28a"></a>`distribution`: `stove0-server`
- <a id="s-09a8ed9b6f"></a>`module`: `stove0_core`
- <a id="s-7a8fc40a65"></a>`name`: `load_selection_artifact`
- <a id="s-d64cfb5aa4"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-0185d11bde"></a>`unit`: `member`

### Declared structure

- <a id="s-ebbb251ae3"></a>`kind`: `"method"`
- <a id="s-c02a52f007"></a>`signature`: `"\"(self, selection_sha256: 'str', artifact_id: 'str') -> 'ArtifactSubject \| None'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-05d4c4a569"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.load_selection_artifact`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 406a089047459c29998d9e34aa12a091b24f8137f37644f2d8162260190c286d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str', artifact_id: 'str') -> 'ArtifactSubject | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_selection_artifact",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
