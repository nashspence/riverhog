# riverhog_client.ApiClient.plan_retrieval

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-plan-retrieval:a70e850921 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f5d7fcb8a9"></a>
- <a id="s-8aa7d071c6"></a>`distribution`: `riverhog-client`
- <a id="s-5529751b12"></a>`module`: `riverhog_client`
- <a id="s-08dacf261a"></a>`name`: `plan_retrieval`
- <a id="s-adde2277f2"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-49e91d9393"></a>`unit`: `member`

### Declared structure

- <a id="s-d6185bee3e"></a>`kind`: `"method"`
- <a id="s-093f8a8483"></a>`signature`: `"\"(self, files: 'Sequence[tuple[int, str]]', *, idempotency_key: 'RetrievalPlanIdempotencyKey \| None' = None, lease_seconds: 'int \| None' = None, restore_policy: 'RestorePolicy' = 'allow') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-c4cb39c1be"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.plan_retrieval`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f63766a32caefc063061f525d8a7a97261faeb806dcc7169912ee5e5a912747b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, files: 'Sequence[tuple[int, str]]', *, idempotency_key: 'RetrievalPlanIdempotencyKey | None' = None, lease_seconds: 'int | None' = None, restore_policy: 'RestorePolicy' = 'allow') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "plan_retrieval",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
