# riverhog_age.CHUNK_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-chunk-size:df452e1425 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b9c62a141"></a>
| Field | Shape |
|---|---|
| <a id="s-d40c99becb"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-2de7243af6"></a>`distribution` | "riverhog-age" |
| <a id="s-e3faab6ff3"></a>`module` | "riverhog_age" |
| <a id="s-cd7c439799"></a>`name` | "CHUNK_SIZE" |
| <a id="s-5fcd1332d8"></a>`unit` | "export" |

## Governing policies

- <a id="pa-9e3d430709"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.CHUNK_SIZE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 30753b02ec087cafcca17c403d37f544dc920250f5249a75012d2904d5e455d2 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 65536
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "CHUNK_SIZE",
  "unit": "export"
}
```
