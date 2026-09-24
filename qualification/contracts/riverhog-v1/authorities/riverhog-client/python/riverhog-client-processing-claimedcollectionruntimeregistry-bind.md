# riverhog_client.processing.ClaimedCollectionRuntimeRegistry.bind

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-8c1eaf2379:dc57ccceaf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cb9f47766e"></a>
- <a id="s-416fa382e0"></a>`distribution`: `riverhog-client`
- <a id="s-9bd3e04ee6"></a>`module`: `riverhog_client.processing`
- <a id="s-b8f4533bb3"></a>`name`: `bind`
- <a id="s-dffe1b2289"></a>`owner`: `riverhog_client.processing.ClaimedCollectionRuntimeRegistry`
- <a id="s-5ce8d655e8"></a>`unit`: `member`

### Declared structure

- <a id="s-932618d514"></a>`kind`: `"method"`
- <a id="s-3803f1fb8d"></a>`signature`: `"\"(self, job_id: 'str', runtime: 'RefreshableClaimedCollectionRuntime') -> 'Iterator[RefreshableClaimedCollectionRuntime]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionRuntimeRegistry](riverhog-client-processing-claimedcollectionruntimeregistry.md)

## Governing policies

- <a id="pa-47003d5862"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionRuntimeRegistry.bind`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d3d2047f700060ee32622ae181f36dfded5492297c5eac421b8d844eb88cee41 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', runtime: 'RefreshableClaimedCollectionRuntime') -> 'Iterator[RefreshableClaimedCollectionRuntime]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "bind",
  "owner": "riverhog_client.processing.ClaimedCollectionRuntimeRegistry",
  "unit": "member"
}
```

</details>
