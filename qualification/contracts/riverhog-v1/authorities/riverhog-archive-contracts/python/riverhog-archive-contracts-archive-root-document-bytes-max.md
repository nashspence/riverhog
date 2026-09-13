# riverhog_archive_contracts.ARCHIVE_ROOT_DOCUMENT_BYTES_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-archive-root-d-1ad9259874:1aeb092b89 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-55eb8421fd"></a>
| Field | Shape |
|---|---|
| <a id="s-13ad85a953"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-de83e6bd1a"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-6f6d1035de"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-4dedb6dbf0"></a>`name` | "ARCHIVE_ROOT_DOCUMENT_BYTES_MAX" |
| <a id="s-7ed3b3cdce"></a>`unit` | "export" |

## Governing policies

- <a id="pa-6415239382"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.ARCHIVE_ROOT_DOCUMENT_BYTES_MAX`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 498ed1a174413cd56c7f4512810b2f6067da5d8e4ce63f5c13281dfe7d69500d -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 65536
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "ARCHIVE_ROOT_DOCUMENT_BYTES_MAX",
  "unit": "export"
}
```
