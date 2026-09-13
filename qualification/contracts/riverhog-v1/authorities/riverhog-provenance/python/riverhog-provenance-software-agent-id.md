# riverhog_provenance.software_agent_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-software-agent-id:710decbec3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-61d27e6302"></a>
| Field | Shape |
|---|---|
| <a id="s-881078863f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-3127249d30"></a>`distribution` | "riverhog-provenance" |
| <a id="s-3acb13ea5e"></a>`module` | "riverhog_provenance" |
| <a id="s-07f9bc3b74"></a>`name` | "software_agent_id" |
| <a id="s-8920e57210"></a>`unit` | "export" |

## Governing policies

- <a id="pa-af3785c990"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.software_agent_id`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cdc81e817a719b8161ccc522a59c3d03d51c050f7394838ea0312cc114c18be6 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(name: 'str') -> 'str'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "software_agent_id",
  "unit": "export"
}
```
