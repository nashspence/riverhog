# riverhog_client.InvalidRange

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-invalidrange:0e44dc6c2e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ca844336d4"></a>
| Field | Shape |
|---|---|
| <a id="s-af5d182890"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ee6adf3bde"></a>`distribution` | "riverhog-client" |
| <a id="s-c1e269d5a1"></a>`module` | "riverhog_client" |
| <a id="s-38a9b09f42"></a>`name` | "InvalidRange" |
| <a id="s-c76a8decbd"></a>`unit` | "export" |

## Governing policies

- <a id="pa-8fd5bcaf47"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.InvalidRange`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 49d93c72ea8926e024a3421cd22ab622302d26f4a099e9e705694d4cdf729265 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "InvalidRange",
  "unit": "export"
}
```
