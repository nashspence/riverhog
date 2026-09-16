# stove0_api_client.Stove0ApiClient.cancel_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-cancel-work:3e5a3232cd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b7dcf878cb"></a>
- <a id="s-adc3dc845e"></a>`distribution`: `stove0-api-client`
- <a id="s-1aefef6e5b"></a>`module`: `stove0_api_client`
- <a id="s-da4ea277cc"></a>`name`: `cancel_work`
- <a id="s-27a07d2c76"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-c299854d77"></a>`unit`: `member`

### Declared structure

- <a id="s-1cfb2106d2"></a>`kind`: `"method"`
- <a id="s-054485abf8"></a>`signature`: `"\"(self, work_id: 'str') -> 'WorkView'\""`

## Maintained corroboration

### Related interface records

- [stove0 work cancel](../../stove0-client/cli/stove0-work-cancel.md)
- [POST /v1/work/{work_id}/cancel](../../stove0/http-operations/post-v1-work-work-id-cancel.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-8ac9be71c5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`
- **Client method:** [reference/stove0/packages/api-client/src/stove0_api_client/client.py::Stove0ApiClient.cancel_work](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L319)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.cancel_work`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98d5932e352740cc616910af26bd535af4afff668c4ea6d00c60373cdab36671 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'WorkView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "cancel_work",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
