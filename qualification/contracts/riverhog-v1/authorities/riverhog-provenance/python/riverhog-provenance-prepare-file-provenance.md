# riverhog_provenance.prepare_file_provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-prepare-file-provenance:4d3e7696cc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-20979b1d81"></a>
| Field | Shape |
|---|---|
| <a id="s-6d5538727b"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-1877a78fdc"></a>`distribution` | "riverhog-provenance" |
| <a id="s-484fd2188b"></a>`module` | "riverhog_provenance" |
| <a id="s-7277a23b38"></a>`name` | "prepare_file_provenance" |
| <a id="s-3642f48186"></a>`unit` | "export" |

## Governing policies

- <a id="pa-75163d6298"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.prepare_file_provenance`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e83f47c146237b81f88b9e26d669eca4a170ee74588bdc0b5cbb29776bb405ab -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(payload: 'Path', *, relative_path: 'str', host_id: 'str', agent_name: 'str', agent_version: 'str', observer: 'FileStateObserver | None' = None, provenance: 'Path | None' = None, omit_reason: 'str | None' = None) -> 'PreparedFileProvenance'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "prepare_file_provenance",
  "unit": "export"
}
```
