# riverhog_client.BadRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-badrequest:72168c0211 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0101916a2c"></a>
| Field | Shape |
|---|---|
| <a id="s-57cf2a527f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-b890e64444"></a>`distribution` | "riverhog-client" |
| <a id="s-b841b465ee"></a>`module` | "riverhog_client" |
| <a id="s-29875a07f2"></a>`name` | "BadRequest" |
| <a id="s-e467ae42f3"></a>`unit` | "export" |

## Governing policies

- <a id="pa-44bc410e8c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.BadRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f33c136cc793137f58cbbc2962620e690c50225c588956c6f49b9583281bde3a -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "BadRequest",
  "unit": "export"
}
```
