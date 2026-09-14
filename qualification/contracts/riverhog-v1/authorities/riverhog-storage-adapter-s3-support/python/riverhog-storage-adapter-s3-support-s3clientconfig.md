# riverhog_storage_adapter_s3_support.S3ClientConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-s3-support:riverhog-storage-adapter-s3-support-s3clientconfig:771452eb5d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-s3-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5b5296e905"></a>
- <a id="s-d29ae1837d"></a>`distribution`: `riverhog-storage-adapter-s3-support`
- <a id="s-b229d8b182"></a>`module`: `riverhog_storage_adapter_s3_support`
- <a id="s-b18467816b"></a>`name`: `S3ClientConfig`
- <a id="s-5efaffd67c"></a>`unit`: `export`

### Declared structure

- <a id="s-dc661352ae"></a>`kind`: `"class"`
- <a id="s-59e13eff8d"></a>`signature`: `"\"(endpoint_url: 'str \| None', region: 'str', access_key_id: 'str', secret_access_key: 'str', session_token: 'str \| None' = None, force_path_style: 'bool' = False) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-f8397eb7df"></a>`endpoint_url` | `'str \| None'` | `required` |
| <a id="s-bb74e60579"></a>`region` | `'str'` | `required` |
| <a id="s-ff24381cee"></a>`access_key_id` | `'str'` | `required` |
| <a id="s-a8fe1a1b59"></a>`secret_access_key` | `'str'` | `required` |
| <a id="s-e554487609"></a>`session_token` | `'str \| None'` | `None` |
| <a id="s-c8267c6281"></a>`force_path_style` | `'bool'` | `False` |

## Governing policies

- <a id="pa-16eded8970"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../../../evidence/sources.md#src-aa14de5031) — `reference/riverhog/storage/s3-support/src/riverhog_storage_adapter_s3_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_s3_support.S3ClientConfig`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4b2ffa5f21b3c6a7a40c8a961ed9b8e904b3fb81109a2f2c826304d4539470bd -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "endpoint_url",
        "type": "'str | None'"
      },
      {
        "default": "required",
        "name": "region",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "access_key_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "secret_access_key",
        "type": "'str'"
      },
      {
        "default": "None",
        "name": "session_token",
        "type": "'str | None'"
      },
      {
        "default": "False",
        "name": "force_path_style",
        "type": "'bool'"
      }
    ],
    "kind": "class",
    "signature": "\"(endpoint_url: 'str | None', region: 'str', access_key_id: 'str', secret_access_key: 'str', session_token: 'str | None' = None, force_path_style: 'bool' = False) -> None\""
  },
  "distribution": "riverhog-storage-adapter-s3-support",
  "module": "riverhog_storage_adapter_s3_support",
  "name": "S3ClientConfig",
  "unit": "export"
}
```
