# riverhog_client.ApiClient.plan_archive_copy_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-plan-archive-co-6be0c76add:7b4c39a8d7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-eb5098a94b"></a>
- <a id="s-a5d5609e5a"></a>`distribution`: `riverhog-client`
- <a id="s-a1ce6c590c"></a>`module`: `riverhog_client`
- <a id="s-374f7726b9"></a>`name`: `plan_archive_copy_retirement`
- <a id="s-f5eeb02596"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-f44f633fcc"></a>`unit`: `member`

### Declared structure

- <a id="s-acfb55ce9e"></a>`kind`: `"method"`
- <a id="s-05514e7a70"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, store: 'ArchiveStoreName') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli archive retire](../../a-riverhog-cli/cli/a-riverhog-cli-archive-retire.md)
- [POST /v1/archive/copies/retirement-plan](../../riverhog/http-operations/post-v1-archive-copies-retirement-plan.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-f1750bfe17"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.plan\_archive\_copy\_retirement](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2532)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.plan_archive_copy_retirement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f9f8467ceffefb83c7b8cb0ba90fa47ea732ba667b8eeafcaec67638d8ff204 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, store: 'ArchiveStoreName') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "plan_archive_copy_retirement",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
