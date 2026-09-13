# riverhog_provenance.PROVENANCE_PROFILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-profile:fc3cca9900 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0dc1d325d8"></a>
| Field | Shape |
|---|---|
| <a id="s-1649eed41b"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-49414c2704"></a>`distribution` | "riverhog-provenance" |
| <a id="s-2c8faee051"></a>`module` | "riverhog_provenance" |
| <a id="s-5c0c93507f"></a>`name` | "PROVENANCE_PROFILE" |
| <a id="s-7b6426e3d3"></a>`unit` | "export" |

## Governing policies

- <a id="pa-afa2f61dc6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_PROFILE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0d11f821786fa471b79714ef7aed24351d0624c80e1cb75d33d2db10922bf7de -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "https://nashspence.github.io/riverhog/v1/provenance"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_PROFILE",
  "unit": "export"
}
```
