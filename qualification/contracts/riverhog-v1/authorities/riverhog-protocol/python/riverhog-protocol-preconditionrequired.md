# riverhog_protocol.PreconditionRequired

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-preconditionrequired:8e8aea608b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-faab5a18fe"></a>
| Field | Shape |
|---|---|
| <a id="s-7d2f531f19"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-4e7ca73ce7"></a>`distribution` | "riverhog-protocol" |
| <a id="s-d242ef1ad9"></a>`module` | "riverhog_protocol" |
| <a id="s-80307784fe"></a>`name` | "PreconditionRequired" |
| <a id="s-cce5fc8a51"></a>`unit` | "export" |

## Governing policies

- <a id="pa-bfcc381aa1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.PreconditionRequired`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eb9a87ff3284daf078b4b952b44061b40cea684e11c694dcc3b7794d4e57eb65 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "PreconditionRequired",
  "unit": "export"
}
```
