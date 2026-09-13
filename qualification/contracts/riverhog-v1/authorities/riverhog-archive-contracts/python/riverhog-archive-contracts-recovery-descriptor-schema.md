# riverhog_archive_contracts.RECOVERY_DESCRIPTOR_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-recovery-descr-c91baf4ac0:4745a6e60f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e680da1301"></a>
| Field | Shape |
|---|---|
| <a id="s-7c2ded1db1"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-826427c433"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-40706fdc2b"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-4c951e9066"></a>`name` | "RECOVERY_DESCRIPTOR_SCHEMA" |
| <a id="s-278756da83"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f4fa156786"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.RECOVERY_DESCRIPTOR_SCHEMA`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c2795dff8a6a0478ee1deff86adfc26c6e60161a40f6c87830b3dc25d85dc44 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-recovery-descriptor/v1"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "RECOVERY_DESCRIPTOR_SCHEMA",
  "unit": "export"
}
```
