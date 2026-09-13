# riverhog_client.HashMismatch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-hashmismatch:29330b11d0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5b56cc54be"></a>
| Field | Shape |
|---|---|
| <a id="s-0690851d8d"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-4db238b559"></a>`distribution` | "riverhog-client" |
| <a id="s-65426af42c"></a>`module` | "riverhog_client" |
| <a id="s-77978b390c"></a>`name` | "HashMismatch" |
| <a id="s-1ab0dc2c06"></a>`unit` | "export" |

## Governing policies

- <a id="pa-936dcb07a0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.HashMismatch`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 198136f458d8e46e97573378106565125398e14a6be243a497c503b4d7048f09 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "HashMismatch",
  "unit": "export"
}
```
