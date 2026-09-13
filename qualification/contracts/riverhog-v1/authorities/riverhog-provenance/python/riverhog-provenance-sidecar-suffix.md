# riverhog_provenance.SIDECAR_SUFFIX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-sidecar-suffix:02e334bd4b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7475bc97a5"></a>
| Field | Shape |
|---|---|
| <a id="s-29f4d4b340"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-1e13f12eda"></a>`distribution` | "riverhog-provenance" |
| <a id="s-5ee1f6074e"></a>`module` | "riverhog_provenance" |
| <a id="s-d96a2fb4a4"></a>`name` | "SIDECAR_SUFFIX" |
| <a id="s-1a8bf2836f"></a>`unit` | "export" |

## Governing policies

- <a id="pa-0509fdb6d4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.SIDECAR_SUFFIX`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aef0828420ee8e0c8e04c7a8840ac6630d4247c6e6b4f2a26c9a31e9cd461c81 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": ".riverhog-provenance.json-seq"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "SIDECAR_SUFFIX",
  "unit": "export"
}
```
