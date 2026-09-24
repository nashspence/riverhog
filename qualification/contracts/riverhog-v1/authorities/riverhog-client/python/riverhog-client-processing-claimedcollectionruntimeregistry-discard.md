# riverhog_client.processing.ClaimedCollectionRuntimeRegistry.discard

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-83479196eb:2848401b66 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f54fa72b0c"></a>
- <a id="s-eb541690fc"></a>`distribution`: `riverhog-client`
- <a id="s-fd82f1b6a8"></a>`module`: `riverhog_client.processing`
- <a id="s-93a3ad3176"></a>`name`: `discard`
- <a id="s-1330487c93"></a>`owner`: `riverhog_client.processing.ClaimedCollectionRuntimeRegistry`
- <a id="s-503a2953fc"></a>`unit`: `member`

### Declared structure

- <a id="s-7920097efc"></a>`kind`: `"method"`
- <a id="s-183e67174e"></a>`signature`: `"\"(self, job_id: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionRuntimeRegistry](riverhog-client-processing-claimedcollectionruntimeregistry.md)

## Governing policies

- <a id="pa-52ae96e733"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionRuntimeRegistry.discard`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb3d396effd63308c7066670974ed4fb921af83ae11004b11488a8367b67a7f8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "discard",
  "owner": "riverhog_client.processing.ClaimedCollectionRuntimeRegistry",
  "unit": "member"
}
```

</details>
