# riverhog_provenance.append_replacement_transformation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-append-replacement-tr-d1ce22b4be:d2085b9d02 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fe9f82eb95"></a>
| Field | Shape |
|---|---|
| <a id="s-38eb46cf6c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-0c6d8a38d1"></a>`distribution` | "riverhog-provenance" |
| <a id="s-04cdfe922d"></a>`module` | "riverhog_provenance" |
| <a id="s-f87b73c2fc"></a>`name` | "append_replacement_transformation" |
| <a id="s-6f30e6c4fb"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f174f560f9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.append_replacement_transformation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56a529e0cff7ff2eaa080b4315fb6070d5cb5d410811fa9cddd9c40509f0e752 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(content: 'bytes', output_path: 'Path', *, relative_path: 'str', host_id: 'str', agent_name: 'str', agent_version: 'str', event_label: 'str', started_at: 'str', ended_at: 'str', observer: 'FileStateObserver', evidence: 'Sequence[Mapping[str, Any]]' = (), policy: 'ObservationPolicy | None' = None) -> 'bytes'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "append_replacement_transformation",
  "unit": "export"
}
```
