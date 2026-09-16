# riverhog_client.ApiClient.get_retrieval_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-retrieval-plan:f32ac2b527 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-357822403f"></a>
- <a id="s-ba641dff9c"></a>`distribution`: `riverhog-client`
- <a id="s-dcd04eb00c"></a>`module`: `riverhog_client`
- <a id="s-f230fd26f9"></a>`name`: `get_retrieval_plan`
- <a id="s-e015225116"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-711b501aa0"></a>`unit`: `member`

### Declared structure

- <a id="s-795f857712"></a>`kind`: `"method"`
- <a id="s-6e9c26c6b7"></a>`signature`: `"\"(self, plan_id: 'str') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [GET /v1/retrieval-plans/{plan_id}](../../riverhog/http-operations/get-v1-retrieval-plans-plan-id.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-0cec9ac241"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/client.py::ApiClient.get_retrieval_plan](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L829)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_retrieval_plan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d3ab944281748286602221f406dfa117beec9de034141671550908f1927a5036 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan_id: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_retrieval_plan",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
