# riverhog_provenance.create_derivative_journal_seed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-create-derivative-journal-seed:e4ec5f37a7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f30d44cdd4"></a>
| Field | Shape |
|---|---|
| <a id="s-8296856f0d"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-3976f0ac85"></a>`distribution` | "riverhog-provenance" |
| <a id="s-df5952d1e4"></a>`module` | "riverhog_provenance" |
| <a id="s-e4ccb4be83"></a>`name` | "create_derivative_journal_seed" |
| <a id="s-0ae6ba6488"></a>`unit` | "export" |

## Governing policies

- <a id="pa-124bca5294"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.create_derivative_journal_seed`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 888a6f92c1a85d8c4171ff4f62475b65c266110c842e10dbaaacc18b1ba89ee8 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, relative_path: 'str', byte_count: 'int', sha256: 'str', agent_name: 'str', agent_version: 'str', event_label: 'str', started_at: 'str', ended_at: 'str', journal_id: 'str') -> 'tuple[bytes, DerivativeJournalSeed]'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "create_derivative_journal_seed",
  "unit": "export"
}
```
