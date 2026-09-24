# a_riverhog_s3_store_lib.S3ClientConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-s3-store-lib:a-riverhog-s3-store-lib-s3clientconfig:523caf6d1c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-s3-store-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-590d7b27ec"></a>
- <a id="s-86e07f0a62"></a>`distribution`: `a-riverhog-s3-store-lib`
- <a id="s-16b56555a0"></a>`module`: `a_riverhog_s3_store_lib`
- <a id="s-e25789b005"></a>`name`: `S3ClientConfig`
- <a id="s-f53a269c70"></a>`unit`: `export`

### Declared structure

- <a id="s-2a422e6412"></a>`kind`: `"class"`
- <a id="s-ea185eb7ce"></a>`signature`: `"\"(endpoint_url: 'str \| None', region: 'str', access_key_id: 'str', secret_access_key: 'str', session_token: 'str \| None' = None, force_path_style: 'bool' = False) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-89cb898f3f"></a>`endpoint_url` | `'str \| None'` | `required` |
| <a id="s-2dd4b246ab"></a>`region` | `'str'` | `required` |
| <a id="s-4148777e03"></a>`access_key_id` | `'str'` | `required` |
| <a id="s-8ab4ca63f4"></a>`secret_access_key` | `'str'` | `required` |
| <a id="s-30d3484558"></a>`session_token` | `'str \| None'` | `None` |
| <a id="s-a1580843ec"></a>`force_path_style` | `'bool'` | `False` |

## Governing policies

- <a id="pa-c9842515bc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-s3-store-lib:a_riverhog_s3_store_lib](../../../evidence/sources/authorities.md#src-231d0d4373) — [some-implementations/riverhog/storage/s3-support/src/a\_riverhog\_s3\_store\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/storage/s3-support/src/a_riverhog_s3_store_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_s3_store_lib.S3ClientConfig`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 38ec9d45bb871cf66c70200b5ff9728bf1d0830a6e034ad735f449b572977afb -->

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
  "distribution": "a-riverhog-s3-store-lib",
  "module": "a_riverhog_s3_store_lib",
  "name": "S3ClientConfig",
  "unit": "export"
}
```

</details>
