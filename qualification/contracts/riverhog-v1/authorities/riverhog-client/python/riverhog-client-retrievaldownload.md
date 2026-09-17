# riverhog_client.RetrievalDownload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-retrievaldownload:ce3587e4dd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7da2405d5d"></a>
- <a id="s-0b2a2735ca"></a>`distribution`: `riverhog-client`
- <a id="s-df95bd2f0d"></a>`module`: `riverhog_client`
- <a id="s-883429b35d"></a>`name`: `RetrievalDownload`
- <a id="s-0562eac125"></a>`unit`: `export`

### Declared structure

- <a id="s-92e35ada95"></a>`kind`: `"class"`
- <a id="s-8e88c11cc2"></a>`signature`: `"\"(collection_id: 'CollectionId', path: 'str', output: 'Path', expected_bytes: 'int', expected_sha256: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-decd4f438d"></a>`collection_id` | `'CollectionId'` | `required` |
| <a id="s-abde27fb30"></a>`path` | `'str'` | `required` |
| <a id="s-44f5b87cc2"></a>`output` | `'Path'` | `required` |
| <a id="s-7708754428"></a>`expected_bytes` | `'int'` | `required` |
| <a id="s-83493ae5f0"></a>`expected_sha256` | `'str'` | `required` |

## Governing policies

- <a id="pa-b89315427c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.RetrievalDownload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa67e1d3eb7b0001dc56cb4493fbb1dd7b1dac1c072d5412530b858aacf249a5 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "collection_id",
        "type": "'CollectionId'"
      },
      {
        "default": "required",
        "name": "path",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "output",
        "type": "'Path'"
      },
      {
        "default": "required",
        "name": "expected_bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "expected_sha256",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(collection_id: 'CollectionId', path: 'str', output: 'Path', expected_bytes: 'int', expected_sha256: 'str') -> None\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "RetrievalDownload",
  "unit": "export"
}
```

</details>
