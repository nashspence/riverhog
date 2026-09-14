# stove0_api_client.Stove0ApiClient.inspect_work_coordination

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-inspect-526260a063:3b6c2a9e30 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f52733641"></a>
- <a id="s-554a0ed19c"></a>`distribution`: `stove0-api-client`
- <a id="s-cc163af94a"></a>`module`: `stove0_api_client`
- <a id="s-fddd46542b"></a>`name`: `inspect_work_coordination`
- <a id="s-1f65f40854"></a>`owner`: `stove0_api_client.Stove0ApiClient`
- <a id="s-62cdba4267"></a>`unit`: `member`

### Declared structure

- <a id="s-73d1a345b8"></a>`kind`: `"method"`
- <a id="s-b0fe39e506"></a>`signature`: `"\"(self, work_id: 'str') -> 'BranchSetEvaluation'\""`

## Maintained corroboration

### Related interface records

- [stove0_api_client.Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-a5fae994e7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.inspect_work_coordination`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 994a752343cf81f545a2ee9bc6bf47c77b4aebd2f64c8e6f30b3cd5f118d27a3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'BranchSetEvaluation'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "inspect_work_coordination",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```
