# stove0_api_client.Stove0ApiClient.get_departure_effect

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-get-dep-63d61dba3c:f73c58cbd0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fd71c92bfd"></a>
- <a id="s-ff999c1132"></a>`distribution`: `stove0-api-client`
- <a id="s-a12b7b4c7e"></a>`module`: `stove0_api_client`
- <a id="s-69ef856a5a"></a>`name`: `get_departure_effect`
- <a id="s-17aa71b2ba"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-1b7674312d"></a>`unit`: `member`

### Declared structure

- <a id="s-efe620d64c"></a>`kind`: `"method"`
- <a id="s-a7ee392e92"></a>`signature`: `"\"(self, departure_id: 'str') -> 'DepartureEffectView'\""`

## Maintained corroboration

### Related interface records

- [stove0 departure show](../../a-stove0-cli/cli/stove0-departure-show.md)
- [GET /v1/departure-effects/{departure_id}](../../stove0/http-operations/get-v1-departure-effects-departure-id.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-0c5f68614d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.get\_departure\_effect](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py#L249)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.get_departure_effect`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d6896778987ca5a9d67c1e15a1f259108bb70dad51ae68dc12f3e2db42a383de -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, departure_id: 'str') -> 'DepartureEffectView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "get_departure_effect",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
