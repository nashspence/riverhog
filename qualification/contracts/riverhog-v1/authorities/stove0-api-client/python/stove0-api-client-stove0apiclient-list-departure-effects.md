# stove0_api_client.Stove0ApiClient.list_departure_effects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-list-de-d1188842e1:adbcbf5fb5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8253ad438e"></a>
- <a id="s-e0fcb8c0e9"></a>`distribution`: `stove0-api-client`
- <a id="s-9a1284659d"></a>`module`: `stove0_api_client`
- <a id="s-55dfdc519c"></a>`name`: `list_departure_effects`
- <a id="s-47e998366f"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-6cc4761a46"></a>`unit`: `member`

### Declared structure

- <a id="s-8cd3e2509c"></a>`kind`: `"method"`
- <a id="s-9010cfce6d"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None) -> 'DepartureEffectPage'\""`

## Maintained corroboration

### Related interface records

- [stove0 departure list](../../a-stove0-cli/cli/stove0-departure-list.md)
- [GET /v1/departure-effects](../../stove0/http-operations/get-v1-departure-effects.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-efcd8400b3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.list\_departure\_effects](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py#L237)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.list_departure_effects`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42511c518c4301fd5ccc84a610ae0bd1cb9c8fd42ed6518d69e4e340b5d27ee1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'DepartureEffectPage'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "list_departure_effects",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
