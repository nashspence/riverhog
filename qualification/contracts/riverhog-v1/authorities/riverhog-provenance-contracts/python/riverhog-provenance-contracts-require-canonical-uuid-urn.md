# riverhog_provenance_contracts.require_canonical_uuid_urn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-contracts:riverhog-provenance-contracts-require-can-6f8a99bdee:2890648b0e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f13bb39b15"></a>
| Field | Shape |
|---|---|
| <a id="s-b7862c55c6"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-02b7962286"></a>`distribution` | "riverhog-provenance-contracts" |
| <a id="s-8bd2c3712c"></a>`module` | "riverhog_provenance_contracts" |
| <a id="s-8b72b97032"></a>`name` | "require_canonical_uuid_urn" |
| <a id="s-8c6922b983"></a>`unit` | "export" |

## Governing policies

- <a id="pa-aebda8b789"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-contracts:riverhog_provenance_contracts](../../../evidence/sources.md#src-9b6289a988) — `packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_contracts.require_canonical_uuid_urn`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5d470e0fdfcb7364233b1bc5ab374786b696d28e00f383f5e30c10f27135eafb -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str', field: 'str' = 'identity') -> 'str'\""
  },
  "distribution": "riverhog-provenance-contracts",
  "module": "riverhog_provenance_contracts",
  "name": "require_canonical_uuid_urn",
  "unit": "export"
}
```
