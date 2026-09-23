# riverhog_client.ApiClient.list_retrieval_plan_files

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-retrieval-plan-files:0a4e5d0fa3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bb1e6f7139"></a>
- <a id="s-6aaef7ff03"></a>`distribution`: `riverhog-client`
- <a id="s-3807ebb6f5"></a>`module`: `riverhog_client`
- <a id="s-4c356f861a"></a>`name`: `list_retrieval_plan_files`
- <a id="s-3802fd2f87"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-9bd626ac4a"></a>`unit`: `member`

### Declared structure

- <a id="s-1414f44742"></a>`kind`: `"method"`
- <a id="s-c10f296959"></a>`signature`: `"\"(self, plan_id: 'str', *, plan_etag: 'str', start_ordinal: 'int' = 0, page_size: 'int' = 100) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli local repair](../../a-riverhog-cli/cli/a-riverhog-cli-local-repair.md)
- [a-riverhog-cli local sync](../../a-riverhog-cli/cli/a-riverhog-cli-local-sync.md)
- [GET /v1/retrieval-plans/{plan_id}/files](../../riverhog/http-operations/get-v1-retrieval-plans-plan-id-files.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-123233bcad"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.list\_retrieval\_plan\_files](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L850)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_retrieval_plan_files`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 549644a447f21c828a55d192aa926cb02df3ea292a767b896bc17fbfc2215d6e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan_id: 'str', *, plan_etag: 'str', start_ordinal: 'int' = 0, page_size: 'int' = 100) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_retrieval_plan_files",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
