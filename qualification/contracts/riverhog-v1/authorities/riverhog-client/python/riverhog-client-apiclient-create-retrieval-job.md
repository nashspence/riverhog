# riverhog_client.ApiClient.create_retrieval_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-create-retrieval-job:a0cd0fd29c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d4fc88f45d"></a>
| Field | Shape |
|---|---|
| <a id="s-ccb6fee832"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-1c10d062e3"></a>`distribution` | "riverhog-client" |
| <a id="s-c8a0f590d8"></a>`module` | "riverhog_client" |
| <a id="s-4291521bf9"></a>`name` | "create_retrieval_job" |
| <a id="s-f94ec4ba29"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-bd92602ac4"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-827141e222"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.create_retrieval_job`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f61643cf0098991a923595e388ff2264ce9b5c32719575711c960cab749ade83 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan_id: 'str', *, plan_etag: 'str', event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_retrieval_job",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
