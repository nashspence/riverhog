# riverhog_client.configured_upload_window

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-configured-upload-window:9a4a607756 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8bb0abdd99"></a>
| Field | Shape |
|---|---|
| <a id="s-6ec9d9b3dd"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-1eac9e4761"></a>`distribution` | "riverhog-client" |
| <a id="s-133f0a7e3f"></a>`module` | "riverhog_client" |
| <a id="s-2e05f4e67c"></a>`name` | "configured_upload_window" |
| <a id="s-91af556b77"></a>`unit` | "export" |

## Governing policies

- <a id="pa-07cfe31ed2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.configured_upload_window`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dfa37fd6d419b59bb31caaa3ebb2acab90e4cfb64b6fe19237750eb12e1676fa -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(values: 'Mapping[str, str] | None' = None, *, concurrency: 'int | None' = None) -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "configured_upload_window",
  "unit": "export"
}
```
