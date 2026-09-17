# riverhog_storage_adapter_support.StorageAdapterHttpResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-e89ba0fbb7:7a8ce6afd6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-93be890247"></a>
- <a id="s-03c581e788"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-5265c1b843"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-4e6aa2f9da"></a>`name`: `StorageAdapterHttpResponse`
- <a id="s-ee21374f34"></a>`unit`: `export`

### Declared structure

- <a id="s-35fed12dcc"></a>`kind`: `"class"`
- <a id="s-aaf21a9637"></a>`signature`: `"\"(status: 'int', headers: 'tuple[tuple[str, str], ...]', body: 'bytes \| Iterator[bytes]') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-0c980a9c30"></a>`status` | `'int'` | `required` |
| <a id="s-ad09a2025c"></a>`headers` | `'tuple[tuple[str, str], ...]'` | `required` |
| <a id="s-b76dc09268"></a>`body` | `'bytes \| Iterator[bytes]'` | `required` |

## Governing policies

- <a id="pa-c3949d780c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterHttpResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f30c97e3310fcdc7b87eebef391a85ee46890a85b7a0acbd761c07c795bcabb -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "status",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "headers",
        "type": "'tuple[tuple[str, str], ...]'"
      },
      {
        "default": "required",
        "name": "body",
        "type": "'bytes | Iterator[bytes]'"
      }
    ],
    "kind": "class",
    "signature": "\"(status: 'int', headers: 'tuple[tuple[str, str], ...]', body: 'bytes | Iterator[bytes]') -> None\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "StorageAdapterHttpResponse",
  "unit": "export"
}
```

</details>
