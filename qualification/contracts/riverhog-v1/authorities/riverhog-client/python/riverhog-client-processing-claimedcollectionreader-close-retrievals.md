# riverhog_client.processing.ClaimedCollectionReader.close_retrievals

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-cada42d25b:a903d44e61 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fa6851b46d"></a>
- <a id="s-a02098eb3b"></a>`distribution`: `riverhog-client`
- <a id="s-33e92649e2"></a>`module`: `riverhog_client.processing`
- <a id="s-079feec28c"></a>`name`: `close_retrievals`
- <a id="s-0e4408c006"></a>`owner`: `riverhog_client.processing.ClaimedCollectionReader`
- <a id="s-d3e043ab95"></a>`unit`: `member`

### Declared structure

- <a id="s-4e598e2419"></a>`kind`: `"method"`
- <a id="s-5a270976a0"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionReader](riverhog-client-processing-claimedcollectionreader.md)

## Governing policies

- <a id="pa-75e66ef343"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionReader.close_retrievals`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 18aa18692b4732ce2e706b1819fb5eead8139226a7a42d9ffd4d07f79377e3c3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "close_retrievals",
  "owner": "riverhog_client.processing.ClaimedCollectionReader",
  "unit": "member"
}
```

</details>
