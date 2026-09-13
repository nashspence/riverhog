# riverhog_provenance_macos_contracts.PLATFORM_FAMILY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-macos-contracts:riverhog-provenance-macos-contracts-platform-family:e3349b4316 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-macos-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9c8182135e"></a>
| Field | Shape |
|---|---|
| <a id="s-ba32816877"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-239d3a652d"></a>`distribution` | "riverhog-provenance-macos-contracts" |
| <a id="s-fbc4de8f2f"></a>`module` | "riverhog_provenance_macos_contracts" |
| <a id="s-1cb7b4ec75"></a>`name` | "PLATFORM_FAMILY" |
| <a id="s-2c2c402de8"></a>`unit` | "export" |

## Governing policies

- <a id="pa-7d37b73708"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-macos-contracts:riverhog_provenance_macos_contracts](../../../evidence/sources.md#src-75fa891e7c) — `reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_macos_contracts.PLATFORM_FAMILY`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c43cfbdd3f56fbb75108c8a4b87b8c2bba68e353426da7409820e8971cecd108 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "macos"
  },
  "distribution": "riverhog-provenance-macos-contracts",
  "module": "riverhog_provenance_macos_contracts",
  "name": "PLATFORM_FAMILY",
  "unit": "export"
}
```
