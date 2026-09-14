# riverhog_archive_contracts.ordered_archive_volume_commitment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-ordered-archiv-1105f7c172:f09a206b12 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dec9a8fa8b"></a>
- <a id="s-0221980a4f"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-ab1c73d8a3"></a>`module`: `riverhog_archive_contracts`
- <a id="s-6d57b83993"></a>`name`: `ordered_archive_volume_commitment`
- <a id="s-d5413de40d"></a>`unit`: `export`

### Declared structure

- <a id="s-80d681d6e3"></a>`kind`: `"function"`
- <a id="s-67c6847ad0"></a>`signature`: `"\"(documents: 'Iterable[ArchiveSequenceDocument]') -> 'str'\""`

## Governing policies

- <a id="pa-4bcb673bd1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.ordered_archive_volume_commitment`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a92c038eb5edfde03edcdea04c2b4bd0f0172ef00b947565d0f711e98abf92fc -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(documents: 'Iterable[ArchiveSequenceDocument]') -> 'str'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "ordered_archive_volume_commitment",
  "unit": "export"
}
```
