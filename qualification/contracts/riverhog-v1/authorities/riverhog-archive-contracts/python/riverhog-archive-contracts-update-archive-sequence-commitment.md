# riverhog_archive_contracts.update_archive_sequence_commitment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-update-archive-45adc8c7f3:f609ae414c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-80cd76c9e4"></a>
- <a id="s-7067f2e082"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-08b25e84b9"></a>`module`: `riverhog_archive_contracts`
- <a id="s-d4bc0d11f8"></a>`name`: `update_archive_sequence_commitment`
- <a id="s-f7c51de3a0"></a>`unit`: `export`

### Declared structure

- <a id="s-42b703a903"></a>`kind`: `"function"`
- <a id="s-85d9d23c5a"></a>`signature`: `"\"(digest: 'Any', document: 'ArchiveSequenceDocument') -> 'None'\""`

## Governing policies

- <a id="pa-4209985778"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.update_archive_sequence_commitment`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5d8595fbf2e29c0ce526db59d27c0f1b017942e7718c9b7c206aeff888bf20a7 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(digest: 'Any', document: 'ArchiveSequenceDocument') -> 'None'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "update_archive_sequence_commitment",
  "unit": "export"
}
```
