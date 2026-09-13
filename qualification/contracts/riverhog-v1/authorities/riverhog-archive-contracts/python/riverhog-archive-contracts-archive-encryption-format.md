# riverhog_archive_contracts.ARCHIVE_ENCRYPTION_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-archive-encryption-format:fe69b79169 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2a561065f5"></a>
| Field | Shape |
|---|---|
| <a id="s-70a80e6002"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-8ab175af5d"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-0c55f604b2"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-e467f731b7"></a>`name` | "ARCHIVE_ENCRYPTION_FORMAT" |
| <a id="s-9450ecb15f"></a>`unit` | "export" |

## Governing policies

- <a id="pa-22697a6163"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.ARCHIVE_ENCRYPTION_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 58363c36f8ed4513b9321063813879acf78d1e506be0564d7b8188fa60811806 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "age-v1-scrypt"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "ARCHIVE_ENCRYPTION_FORMAT",
  "unit": "export"
}
```
