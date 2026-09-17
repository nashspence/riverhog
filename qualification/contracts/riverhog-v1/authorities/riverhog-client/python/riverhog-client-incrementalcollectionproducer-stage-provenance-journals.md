# riverhog_client.IncrementalCollectionProducer.stage_provenance_journals

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-incrementalcollectionprod-6f52772652:e04ceb88b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b5ac9e8f90"></a>
- <a id="s-2ff826d7d2"></a>`distribution`: `riverhog-client`
- <a id="s-0a9af9b426"></a>`module`: `riverhog_client`
- <a id="s-9dd6e6eac1"></a>`name`: `stage_provenance_journals`
- <a id="s-f1e1c92662"></a>`owner`: `riverhog_client.IncrementalCollectionProducer`
- <a id="s-89f1f2b21e"></a>`unit`: `member`

### Declared structure

- <a id="s-486e6be0d7"></a>`kind`: `"method"`
- <a id="s-749989b1d6"></a>`signature`: `"\"(self, journals: 'Iterable[tuple[str, bytes]]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [IncrementalCollectionProducer](riverhog-client-incrementalcollectionproducer.md)

## Governing policies

- <a id="pa-2ee9baca0e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.IncrementalCollectionProducer.stage_provenance_journals`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61ffcfa7551396d56bb2f0dc5a3115e68243cd36102017ddd65a489a7d8aa28c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, journals: 'Iterable[tuple[str, bytes]]') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "stage_provenance_journals",
  "owner": "riverhog_client.IncrementalCollectionProducer",
  "unit": "member"
}
```

</details>
