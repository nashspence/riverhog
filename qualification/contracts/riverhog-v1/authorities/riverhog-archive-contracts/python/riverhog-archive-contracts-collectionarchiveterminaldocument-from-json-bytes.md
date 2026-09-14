# riverhog_archive_contracts.CollectionArchiveTerminalDocument.from_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectionarch-ec212341f8:80d940492f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a56d956ee7"></a>
- <a id="s-c6933c9749"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-4ea84bd6b1"></a>`module`: `riverhog_archive_contracts`
- <a id="s-97be2e21d2"></a>`name`: `from_json_bytes`
- <a id="s-754e44f5d4"></a>`owner`: `riverhog_archive_contracts.CollectionArchiveTerminalDocument`
- <a id="s-86dc60b5f8"></a>`unit`: `member`

### Declared structure

- <a id="s-f72b4a11ad"></a>`kind`: `"classmethod"`
- <a id="s-174fd885f7"></a>`signature`: `"\"(cls, content: 'bytes \| str') -> 'CollectionArchiveTerminalDocument'\""`

## Maintained corroboration

### Related interface records

- [riverhog_archive_contracts.CollectionArchiveTerminalDocument](riverhog-archive-contracts-collectionarchiveterminaldocument.md)

## Governing policies

- <a id="pa-0a0141bcb0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionArchiveTerminalDocument.from_json_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 16f0206f3cfb9b6e677323d5a35017e34cea30dfff26ec3702f873e02589f875 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, content: 'bytes | str') -> 'CollectionArchiveTerminalDocument'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "from_json_bytes",
  "owner": "riverhog_archive_contracts.CollectionArchiveTerminalDocument",
  "unit": "member"
}
```
