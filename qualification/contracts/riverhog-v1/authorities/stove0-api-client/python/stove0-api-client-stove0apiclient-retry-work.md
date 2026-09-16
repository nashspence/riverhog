# stove0_api_client.Stove0ApiClient.retry_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-retry-work:51e24d93f4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c687ff3572"></a>
- <a id="s-620144c6d8"></a>`distribution`: `stove0-api-client`
- <a id="s-87f92ee99f"></a>`module`: `stove0_api_client`
- <a id="s-4798123beb"></a>`name`: `retry_work`
- <a id="s-e8b9bc6c7d"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-23ad7b957d"></a>`unit`: `member`

### Declared structure

- <a id="s-13b30fa2af"></a>`kind`: `"method"`
- <a id="s-f1c0540519"></a>`signature`: `"\"(self, work_id: 'str') -> 'WorkView'\""`

## Maintained corroboration

### Related interface records

- [stove0 work retry](../../stove0-client/cli/stove0-work-retry.md)
- [POST /v1/work/{work_id}/retry](../../stove0/http-operations/post-v1-work-work-id-retry.md)
- [Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-cccf51e98a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`
- **Client method:** [reference/stove0/packages/api-client/src/stove0_api_client/client.py::Stove0ApiClient.retry_work](../../../../../../reference/stove0/packages/api-client/src/stove0_api_client/client.py#L314)

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.retry_work`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 140c93a78764693155d2fbd666d2cffd3aabb78b024c6a34b8ecee9c7e59ecc6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'WorkView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "retry_work",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```
