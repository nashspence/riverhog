# riverhog_archive_contracts.CollectionArchiveTerminalDocument.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectionarch-9dbd502c44:f792523249 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9996781649"></a>
- <a id="s-4a33ae7cb9"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-8890b8f184"></a>`module`: `riverhog_archive_contracts`
- <a id="s-0b9c9a37af"></a>`name`: `from_mapping`
- <a id="s-a85d0a4d20"></a>`owner`: `riverhog_archive_contracts.CollectionArchiveTerminalDocument`
- <a id="s-1d3f9a1dce"></a>`unit`: `member`

### Declared structure

- <a id="s-e559577e2b"></a>`kind`: `"classmethod"`
- <a id="s-6c6787feb3"></a>`signature`: `"\"(cls, value: 'object') -> 'CollectionArchiveTerminalDocument'\""`

## Maintained corroboration

### Related interface records

- [CollectionArchiveTerminalDocument](riverhog-archive-contracts-collectionarchiveterminaldocument.md)

## Governing policies

- <a id="pa-281d78cf28"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionArchiveTerminalDocument.from_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 72d9c5830865cc86fda37b03cfec2d15e194fd7cca2026dae797abba538d1698 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'object') -> 'CollectionArchiveTerminalDocument'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "from_mapping",
  "owner": "riverhog_archive_contracts.CollectionArchiveTerminalDocument",
  "unit": "member"
}
```

</details>
