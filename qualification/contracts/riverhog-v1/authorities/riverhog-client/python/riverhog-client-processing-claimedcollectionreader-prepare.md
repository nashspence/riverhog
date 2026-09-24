# riverhog_client.processing.ClaimedCollectionReader.prepare

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-6d3b3deb3c:b318ccb9fb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4bcd22caf"></a>
- <a id="s-cc12e0e49b"></a>`distribution`: `riverhog-client`
- <a id="s-84a79c7623"></a>`module`: `riverhog_client.processing`
- <a id="s-e54003e096"></a>`name`: `prepare`
- <a id="s-5a5150439e"></a>`owner`: `riverhog_client.processing.ClaimedCollectionReader`
- <a id="s-5ecb62552d"></a>`unit`: `member`

### Declared structure

- <a id="s-2c943d14e6"></a>`kind`: `"method"`
- <a id="s-9b7d59cd51"></a>`signature`: `"\"(self, artifacts: 'Sequence[ClaimedArtifact] \| None' = None, *, lease_seconds: 'int' = 1800, restore_policy: 'RetrievalPolicy' = 'available-only', poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'ClaimedRetrieval'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionReader](riverhog-client-processing-claimedcollectionreader.md)

## Governing policies

- <a id="pa-75da06eae9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionReader.prepare`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8d30ca40182467dfb95c1e16441aec6aa451acee9ae72bdc0e254ec08290ce28 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, artifacts: 'Sequence[ClaimedArtifact] | None' = None, *, lease_seconds: 'int' = 1800, restore_policy: 'RetrievalPolicy' = 'available-only', poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'ClaimedRetrieval'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "prepare",
  "owner": "riverhog_client.processing.ClaimedCollectionReader",
  "unit": "member"
}
```

</details>
