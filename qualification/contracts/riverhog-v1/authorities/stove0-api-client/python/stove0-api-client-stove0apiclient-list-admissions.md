# stove0_api_client.Stove0ApiClient.list_admissions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-list-admissions:df341a8ccf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b966fd28ea"></a>
- <a id="s-5bbb49e44d"></a>`distribution`: `stove0-api-client`
- <a id="s-65c159115b"></a>`module`: `stove0_api_client`
- <a id="s-c092deac78"></a>`name`: `list_admissions`
- <a id="s-164bf5f9da"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-8eb2ce2088"></a>`unit`: `member`

### Declared structure

- <a id="s-aae8156ef9"></a>`kind`: `"method"`
- <a id="s-f11f7b1177"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None, policy_id: 'str \| None' = None, state: 'AdmissionState \| None' = None, query: 'str \| None' = None, sort: 'AdmissionSort' = 'created_at', order: 'SortOrder' = 'desc') -> 'AdmissionPage'\""`

## Maintained corroboration

### Related interface records

- [stove0 admission list](../../a-stove0-cli/cli/stove0-admission-list.md)
- [GET /v1/admissions](../../stove0/http-operations/get-v1-admissions.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-635119389f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.list\_admissions](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py#L182)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.list_admissions`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a82638be528febb527122cb6c4c1b1921c2421ad2b0a5cbbf2c2f8863cf41d47 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, policy_id: 'str | None' = None, state: 'AdmissionState | None' = None, query: 'str | None' = None, sort: 'AdmissionSort' = 'created_at', order: 'SortOrder' = 'desc') -> 'AdmissionPage'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "list_admissions",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
