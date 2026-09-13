# riverhog_archive_contracts.RecoveryDescriptor.to_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-recoverydescri-a0c0551eb3:8812992842 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4932e90ecb"></a>
| Field | Shape |
|---|---|
| <a id="s-c602c17208"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-4eb08b6b71"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-ebd2c41ef4"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-88804cd6d5"></a>`name` | "to_json_bytes" |
| <a id="s-425fe2d6ce"></a>`owner` | "riverhog_archive_contracts.RecoveryDescriptor" |
| <a id="s-ddff61dd70"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_archive_contracts.RecoveryDescriptor](riverhog-archive-contracts-recoverydescriptor.md)

## Governing policies

- <a id="pa-cbef4115b0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.RecoveryDescriptor.to_json_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13b076d6f0381478dcec034920764934f97d6f6c3600446145b60a905b3e5c2a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "to_json_bytes",
  "owner": "riverhog_archive_contracts.RecoveryDescriptor",
  "unit": "member"
}
```
