# riverhog_archive_contracts.normalize_passphrase_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-normalize-passphrase-id:a8b084988c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-06e9442d81"></a>
- <a id="s-5af11361fd"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-59561679da"></a>`module`: `riverhog_archive_contracts`
- <a id="s-007ea07090"></a>`name`: `normalize_passphrase_id`
- <a id="s-6ada3a1622"></a>`unit`: `export`

### Declared structure

- <a id="s-6e10c051cf"></a>`kind`: `"function"`
- <a id="s-74204de7e5"></a>`signature`: `"\"(value: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-805279e12c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.normalize_passphrase_id`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 617de235a37e0c19d8b92000d75796dd72253b029e1b6f54417a88caccc863ba -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "normalize_passphrase_id",
  "unit": "export"
}
```
