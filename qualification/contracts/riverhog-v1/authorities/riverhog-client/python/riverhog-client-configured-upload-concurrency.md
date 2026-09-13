# riverhog_client.configured_upload_concurrency

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-configured-upload-concurrency:c0e7b1c803 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d7c4e25535"></a>
| Field | Shape |
|---|---|
| <a id="s-2252568801"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a1943d0a75"></a>`distribution` | "riverhog-client" |
| <a id="s-4cb09f3b6d"></a>`module` | "riverhog_client" |
| <a id="s-b8efb8b34b"></a>`name` | "configured_upload_concurrency" |
| <a id="s-dfa46f34c8"></a>`unit` | "export" |

## Governing policies

- <a id="pa-cd86ebeb6c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.configured_upload_concurrency`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f93d20bf2b20cb610b63fcd947d4ce303668dee21adea0ab4ebb4efb68dcf1ac -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(values: 'Mapping[str, str] | None' = None) -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "configured_upload_concurrency",
  "unit": "export"
}
```
