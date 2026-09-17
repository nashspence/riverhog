# riverhog_client.transform.CollectionTransformRuntime.refresh_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-collectiontrans-8f73f38c6f:6e89f5f018 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f1f1cd4b4a"></a>
- <a id="s-f8dac250b6"></a>`distribution`: `riverhog-client`
- <a id="s-fc52fdbfa5"></a>`module`: `riverhog_client.transform`
- <a id="s-25634cd905"></a>`name`: `refresh_capability`
- <a id="s-71b76f943c"></a>`owner`: `riverhog_client.transform.CollectionTransformRuntime`
- <a id="s-4171a0a6e9"></a>`unit`: `member`

### Declared structure

- <a id="s-c4e00af591"></a>`kind`: `"method"`
- <a id="s-e266c71fcf"></a>`signature`: `"\"(self, capability_token: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-transform-collectiontransformruntime.md)

## Governing policies

- <a id="pa-096b968f4e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.CollectionTransformRuntime.refresh_capability`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 307f4eae7a66a16930ea8d429060f34675c17bc7e730817b0bf1f3685dfd1c37 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, capability_token: 'str') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "refresh_capability",
  "owner": "riverhog_client.transform.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
