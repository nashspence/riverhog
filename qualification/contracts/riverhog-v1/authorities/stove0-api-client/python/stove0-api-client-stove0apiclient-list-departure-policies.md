# stove0_api_client.Stove0ApiClient.list_departure_policies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-list-de-703a539311:522003ffb0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-308e8650bf"></a>
- <a id="s-826f0f6ed4"></a>`distribution`: `stove0-api-client`
- <a id="s-380de00d51"></a>`module`: `stove0_api_client`
- <a id="s-faf252eac0"></a>`name`: `list_departure_policies`
- <a id="s-e5f8636d5d"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-cb1254c885"></a>`unit`: `member`

### Declared structure

- <a id="s-b22249aa35"></a>`kind`: `"method"`
- <a id="s-dc395e0e2e"></a>`signature`: `"\"(self) -> 'DeparturePolicyCatalogView'\""`

## Maintained corroboration

### Related interface records

- [stove0 departure policy list](../../a-stove0-cli/cli/stove0-departure-policy-list.md)
- [GET /v1/departure-policies](../../stove0/http-operations/get-v1-departure-policies.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-b16675f1dc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.list\_departure\_policies](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py#L223)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.list_departure_policies`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f119f1d9cf1307d0e0eba57f6f8b4aeb56ecbb3c5459094e43fd3d441d2a8c3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'DeparturePolicyCatalogView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "list_departure_policies",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
