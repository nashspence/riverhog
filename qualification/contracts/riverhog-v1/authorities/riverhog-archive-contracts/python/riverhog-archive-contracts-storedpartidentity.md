# riverhog_archive_contracts.StoredPartIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-storedpartidentity:a425c559ef -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-69d4a57857"></a>
| Field | Shape |
|---|---|
| <a id="s-dc6fb593f1"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-c52d63e601"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-c12451a567"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-91b1ceee6e"></a>`name` | "StoredPartIdentity" |
| <a id="s-8f97a51f7f"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_archive_contracts.StoredPartIdentity.to_mapping](riverhog-archive-contracts-storedpartidentity-to-mapping.md)
- [riverhog_archive_contracts.StoredPartIdentity.from_mapping](riverhog-archive-contracts-storedpartidentity-from-mapping.md)

## Governing policies

- <a id="pa-05e13ad410"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.StoredPartIdentity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e036e36ee35a43a7934337a7d0ce44d7aaf6f43794c7e7a0b5243ce6fe506d94 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "number",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "plaintext_start",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "plaintext_bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "plaintext_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "stored_bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "stored_sha256",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(number: 'int', plaintext_start: 'int', plaintext_bytes: 'int', plaintext_sha256: 'str', stored_bytes: 'int', stored_sha256: 'str') -> None\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "StoredPartIdentity",
  "unit": "export"
}
```
