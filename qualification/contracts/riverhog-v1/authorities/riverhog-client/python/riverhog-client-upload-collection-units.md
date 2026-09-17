# riverhog_client.upload_collection_units

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-upload-collection-units:894de3072e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5e32dfab90"></a>
- <a id="s-ee39d23aa3"></a>`distribution`: `riverhog-client`
- <a id="s-fb7b417af9"></a>`module`: `riverhog_client`
- <a id="s-17a4293067"></a>`name`: `upload_collection_units`
- <a id="s-3ae7b657a1"></a>`unit`: `export`

### Declared structure

- <a id="s-c4fbdc4a8b"></a>`kind`: `"function"`
- <a id="s-8bf2a668a3"></a>`signature`: `"\"(api: 'CollectionUnitApi', collection_id: 'CollectionId', *, content_for_unit: 'UnitContent', concurrency: 'int', window: 'int', client_factory: 'Callable[[], CollectionUnitApi] \| None' = None, on_committed: 'UploadProgress \| None' = None, on_resumed: 'UploadProgress \| None' = None, retry_notice: 'RetryNotice \| None' = None, cancel_check: 'Callable[[], None] \| None' = None) -> 'int'\""`

## Governing policies

- <a id="pa-254df210b5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.upload_collection_units`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b49d0e5bd85059d46ef3749f25544bfafed048b7e36238cc0f7ff159538f50a4 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(api: 'CollectionUnitApi', collection_id: 'CollectionId', *, content_for_unit: 'UnitContent', concurrency: 'int', window: 'int', client_factory: 'Callable[[], CollectionUnitApi] | None' = None, on_committed: 'UploadProgress | None' = None, on_resumed: 'UploadProgress | None' = None, retry_notice: 'RetryNotice | None' = None, cancel_check: 'Callable[[], None] | None' = None) -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "upload_collection_units",
  "unit": "export"
}
```

</details>
