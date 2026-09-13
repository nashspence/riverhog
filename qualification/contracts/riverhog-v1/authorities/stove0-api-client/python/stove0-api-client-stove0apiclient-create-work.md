# stove0_api_client.Stove0ApiClient.create_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-stove0apiclient-create-work:559b8d016b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3857ec2c1e"></a>
| Field | Shape |
|---|---|
| <a id="s-56c171cdbd"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-0b3835fae3"></a>`distribution` | "stove0-api-client" |
| <a id="s-21cd6d751c"></a>`module` | "stove0_api_client" |
| <a id="s-32ad71a68b"></a>`name` | "create_work" |
| <a id="s-74f344f7ec"></a>`owner` | "stove0_api_client.Stove0ApiClient" |
| <a id="s-8b712b0905"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_api_client.Stove0ApiClient](stove0-api-client-stove0apiclient.md)

## Governing policies

- <a id="pa-e0fca2f862"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_api_client.Stove0ApiClient.create_work`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 83a40b0472f1b7d2dd16f7e0fa446863ddf56c3938b630bf8f720489f67a75fb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, recipe_id: 'str', inputs: 'Sequence[CollectionRootRef]', *, preview_sha256: 'str', recipe_revision: 'int | None' = None, effective_intent: 'Mapping[str, Any] | None' = None) -> 'WorkView'\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "create_work",
  "owner": "stove0_api_client.Stove0ApiClient",
  "unit": "member"
}
```
