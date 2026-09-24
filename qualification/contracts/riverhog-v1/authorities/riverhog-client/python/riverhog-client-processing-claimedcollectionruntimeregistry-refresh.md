# riverhog_client.processing.ClaimedCollectionRuntimeRegistry.refresh

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-3698b2e1e3:d4dc654476 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7a2067552e"></a>
- <a id="s-cfc3080098"></a>`distribution`: `riverhog-client`
- <a id="s-29cdbf1701"></a>`module`: `riverhog_client.processing`
- <a id="s-c75e5b07b2"></a>`name`: `refresh`
- <a id="s-1e152c7444"></a>`owner`: `riverhog_client.processing.ClaimedCollectionRuntimeRegistry`
- <a id="s-d65a888f70"></a>`unit`: `member`

### Declared structure

- <a id="s-7c7fa1c996"></a>`kind`: `"method"`
- <a id="s-096d1930f0"></a>`signature`: `"\"(self, job_id: 'str', capability_token: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionRuntimeRegistry](riverhog-client-processing-claimedcollectionruntimeregistry.md)

## Governing policies

- <a id="pa-3ddcafb163"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionRuntimeRegistry.refresh`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6266642cca97da03bfadc00fb72bf12bb19d9b4793d9455c4e40f8539f720473 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', capability_token: 'str') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "refresh",
  "owner": "riverhog_client.processing.ClaimedCollectionRuntimeRegistry",
  "unit": "member"
}
```

</details>
