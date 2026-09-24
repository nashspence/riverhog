# riverhog_client.processing.ClaimedCollectionRuntime.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-972dabedad:6fadca9856 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0da3927527"></a>
- <a id="s-e214d872f3"></a>`distribution`: `riverhog-client`
- <a id="s-da18e069d5"></a>`module`: `riverhog_client.processing`
- <a id="s-b3baaffae5"></a>`name`: `__exit__`
- <a id="s-3421e0283d"></a>`owner`: `riverhog_client.processing.ClaimedCollectionRuntime`
- <a id="s-2429a6e4c9"></a>`unit`: `member`

### Declared structure

- <a id="s-47a0680b82"></a>`kind`: `"method"`
- <a id="s-ae89997bbc"></a>`signature`: `"\"(self, _exc_type: 'object', _exc: 'object', _tb: 'object') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionRuntime](riverhog-client-processing-claimedcollectionruntime.md)

## Governing policies

- <a id="pa-21a8f396f2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionRuntime.__exit__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9dd7c7cef9ea51abbc866228c270aa47abc3e8d713f13edfaee07aab8db550b0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, _exc_type: 'object', _exc: 'object', _tb: 'object') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "__exit__",
  "owner": "riverhog_client.processing.ClaimedCollectionRuntime",
  "unit": "member"
}
```

</details>
