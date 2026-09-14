# riverhog_client.put_collection_upload_unit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-put-collection-upload-unit:bfaf1c32d5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7cb43131c3"></a>
- <a id="s-e1c21d6049"></a>`distribution`: `riverhog-client`
- <a id="s-b93a1f5902"></a>`module`: `riverhog_client`
- <a id="s-bb844fd13a"></a>`name`: `put_collection_upload_unit`
- <a id="s-c3ca270d90"></a>`unit`: `export`

### Declared structure

- <a id="s-2c301c4024"></a>`kind`: `"function"`
- <a id="s-d3403cbd0a"></a>`signature`: `"\"(api: 'CollectionUnitApi', collection_id: 'CollectionId', assignment: 'CollectionUploadUnitAssignmentDocument', *, content_for_unit: 'UnitContent', retry_notice: 'RetryNotice \| None' = None, retry_initial_delay_seconds: 'float' = 1.0, retry_max_delay_seconds: 'float' = 10.0) -> 'int'\""`

## Governing policies

- <a id="pa-ff0b4b05b7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.put_collection_upload_unit`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 607dd66838b9383ce5d4d1273f682a25920ed4e756f7e2d10f6f5e4fb5fdcbab -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(api: 'CollectionUnitApi', collection_id: 'CollectionId', assignment: 'CollectionUploadUnitAssignmentDocument', *, content_for_unit: 'UnitContent', retry_notice: 'RetryNotice | None' = None, retry_initial_delay_seconds: 'float' = 1.0, retry_max_delay_seconds: 'float' = 10.0) -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "put_collection_upload_unit",
  "unit": "export"
}
```
