# riverhog_protocol.RiverhogError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-riverhogerror:c39f6a153d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6582fb5d84"></a>
| Field | Shape |
|---|---|
| <a id="s-ca5c3eae88"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ed097549c3"></a>`distribution` | "riverhog-protocol" |
| <a id="s-e0f32d5d5f"></a>`module` | "riverhog_protocol" |
| <a id="s-14798d0c52"></a>`name` | "RiverhogError" |
| <a id="s-fd2f7645e9"></a>`unit` | "export" |

## Governing policies

- <a id="pa-cd566ea45f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RiverhogError`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b0bd4c5ad62d9691dd1329da77b1a7081a8e0d8ef2cc2381759413e8fb70569a -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RiverhogError",
  "unit": "export"
}
```
