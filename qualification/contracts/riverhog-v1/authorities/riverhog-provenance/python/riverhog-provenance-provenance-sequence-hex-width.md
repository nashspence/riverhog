# riverhog_provenance.PROVENANCE_SEQUENCE_HEX_WIDTH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-sequence-hex-width:d215cbce4d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-596d1cbdaa"></a>
| Field | Shape |
|---|---|
| <a id="s-8d3553c6ae"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-8061b7b7b2"></a>`distribution` | "riverhog-provenance" |
| <a id="s-cb4b9a098b"></a>`module` | "riverhog_provenance" |
| <a id="s-71a8fb07fc"></a>`name` | "PROVENANCE_SEQUENCE_HEX_WIDTH" |
| <a id="s-e82779a6cb"></a>`unit` | "export" |

## Governing policies

- <a id="pa-1260f3870c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_SEQUENCE_HEX_WIDTH`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea0fa2ed5a0f13ac819be824293d66d9bb647b78246b8ca38829062dff10af95 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 64
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_SEQUENCE_HEX_WIDTH",
  "unit": "export"
}
```
