# riverhog_provenance.create_derivative_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-create-derivative-journal:8ed9f2d3fa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-69894b27f5"></a>
| Field | Shape |
|---|---|
| <a id="s-e0b9659b2b"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-bd537146aa"></a>`distribution` | "riverhog-provenance" |
| <a id="s-94a280238b"></a>`module` | "riverhog_provenance" |
| <a id="s-037a9de802"></a>`name` | "create_derivative_journal" |
| <a id="s-2a0b93ead2"></a>`unit` | "export" |

## Governing policies

- <a id="pa-54ac180646"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.create_derivative_journal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1354cf964839093e56559c5c70fd151c1a795af5465eacd7f5bb6f6747177c59 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(output_path: 'Path', *, relative_path: 'str', source_journals: 'Sequence[bytes]', host_id: 'str', agent_name: 'str', agent_version: 'str', event_label: 'str', started_at: 'str', ended_at: 'str', observer: 'FileStateObserver', derivation_kind: 'str' = 'transformation', evidence: 'Sequence[Mapping[str, Any]]' = (), policy: 'ObservationPolicy | None' = None) -> 'bytes'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "create_derivative_journal",
  "unit": "export"
}
```
