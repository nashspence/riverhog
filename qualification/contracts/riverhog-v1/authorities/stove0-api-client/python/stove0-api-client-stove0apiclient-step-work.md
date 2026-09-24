# stove0_api_client.Stove0ApiClient.step_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-step-work:ef51fcef24 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-16db260355"></a>
- <a id="s-36acced023"></a>`distribution`: `stove0-api-client`
- <a id="s-f3db7a90a7"></a>`module`: `stove0_api_client`
- <a id="s-8e45fd133c"></a>`name`: `step_work`
- <a id="s-f7c7725509"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-fbdb5f8dd2"></a>`unit`: `member`

### Declared structure

- <a id="s-44047826c5"></a>`kind`: `"method"`
- <a id="s-45a588db76"></a>`signature`: `"\"(self, work_id: 'str') -> 'WorkView'\""`

## Maintained corroboration

### Related interface records

- [stove0 work step](../../a-stove0-cli/cli/stove0-work-step.md)
- [POST /v1/work/{work_id}/step](../../stove0/http-operations/post-v1-work-work-id-step.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-229021222e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources/authorities.md#src-5d52ac5998) — [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/__init__.py)
- **Client method:** [some-implementations/stove0/packages/api-client/src/stove0\_api\_client/client.py::Stove0ApiClient.step\_work](../../../../../../some-implementations/stove0/packages/api-client/src/stove0_api_client/client.py#L350)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.step_work`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 463fb0500cb59dcdf73a6db1765576b8a1a3b5cabac857994d1b4c928748ed0a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'WorkView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "step_work",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```

</details>
