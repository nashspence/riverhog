# riverhog_client.transform.ClaimedCollectionRuntimeRegistry.refresh

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-f7b79a9112:960fa400b1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-508450abce"></a>
- <a id="s-5d1878ed80"></a>`distribution`: `riverhog-client`
- <a id="s-f10f279bea"></a>`module`: `riverhog_client.transform`
- <a id="s-b5f72894ee"></a>`name`: `refresh`
- <a id="s-21dce42628"></a>`owner`: `riverhog_client.transform.ClaimedCollectionRuntimeRegistry`
- <a id="s-8ffcccf52d"></a>`unit`: `member`

### Declared structure

- <a id="s-830b0e4e24"></a>`kind`: `"method"`
- <a id="s-34d7f2da18"></a>`signature`: `"\"(self, job_id: 'str', capability_token: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionRuntimeRegistry](riverhog-client-transform-claimedcollectionruntimeregistry.md)

## Governing policies

- <a id="pa-4b037f0826"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionRuntimeRegistry.refresh`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5324bc0c35570ce65979f258427749eba2a6a7de6725c008457f5e1f26c534b4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, job_id: 'str', capability_token: 'str') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "refresh",
  "owner": "riverhog_client.transform.ClaimedCollectionRuntimeRegistry",
  "unit": "member"
}
```

</details>
