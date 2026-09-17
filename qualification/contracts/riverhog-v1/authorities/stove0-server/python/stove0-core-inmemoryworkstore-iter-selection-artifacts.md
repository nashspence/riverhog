# stove0_core.InMemoryWorkStore.iter_selection_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-iter-select-bb7daa5513:70ab4dc26d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-025a3dbec1"></a>
- <a id="s-2f32d47794"></a>`distribution`: `stove0-server`
- <a id="s-4020f899be"></a>`module`: `stove0_core`
- <a id="s-20bf3faeee"></a>`name`: `iter_selection_artifacts`
- <a id="s-667aab917f"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-2e7d4e7b51"></a>`unit`: `member`

### Declared structure

- <a id="s-ff74cff0a7"></a>`kind`: `"method"`
- <a id="s-caff2a57eb"></a>`signature`: `"\"(self, selection_sha256: 'str') -> 'Iterator[ArtifactSubject]'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-d069d3ed19"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.iter_selection_artifacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c15fbec783100fec166e062697b03fa1c937588de38d53844feabc631c2da00f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str') -> 'Iterator[ArtifactSubject]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_selection_artifacts",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
