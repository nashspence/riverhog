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
| Field | Shape |
|---|---|
| <a id="s-917aff0b8a"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-0b2a2735ca"></a>`distribution` | "riverhog-client" |
| <a id="s-df95bd2f0d"></a>`module` | "riverhog_client" |
| <a id="s-883429b35d"></a>`name` | "RetrievalDownload" |
| <a id="s-0562eac125"></a>`unit` | "export" |

## Governing policies

- <a id="pa-b89315427c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.RetrievalDownload`

### Exact owned JSON

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
