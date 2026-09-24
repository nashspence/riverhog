# riverhog_client.processing.ClaimedCollectionReader.replace_api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-36f77f66d6:938cfa9a99 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0f383ca60e"></a>
- <a id="s-01247f1f78"></a>`distribution`: `riverhog-client`
- <a id="s-80b6b1c7a2"></a>`module`: `riverhog_client.processing`
- <a id="s-8e63a9b883"></a>`name`: `replace_api`
- <a id="s-e2456ffe58"></a>`owner`: `riverhog_client.processing.ClaimedCollectionReader`
- <a id="s-7c0e42939b"></a>`unit`: `member`

### Declared structure

- <a id="s-2b9b96789f"></a>`kind`: `"method"`
- <a id="s-6977a687bb"></a>`signature`: `"\"(self, api: 'ClaimedCollectionApi') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionReader](riverhog-client-processing-claimedcollectionreader.md)

## Governing policies

- <a id="pa-be9a3e7d0f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionReader.replace_api`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6f6d6789dd7e021a848cc0ad7387b4d26cdcc8e4619636e575af3e70a2b2596c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, api: 'ClaimedCollectionApi') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "replace_api",
  "owner": "riverhog_client.processing.ClaimedCollectionReader",
  "unit": "member"
}
```

</details>
