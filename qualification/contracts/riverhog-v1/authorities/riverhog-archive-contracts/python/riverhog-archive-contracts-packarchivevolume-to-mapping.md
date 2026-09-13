# riverhog_archive_contracts.PackArchiveVolume.to_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-packarchivevol-941bf80e45:258778019a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-507bcd2fb0"></a>
| Field | Shape |
|---|---|
| <a id="s-5941650974"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-3d616cc99a"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-4e7e1eade5"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-76015ceca0"></a>`name` | "to_mapping" |
| <a id="s-002524fd8c"></a>`owner` | "riverhog_archive_contracts.PackArchiveVolume" |
| <a id="s-2d7ad3fa5f"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_archive_contracts.PackArchiveVolume](riverhog-archive-contracts-packarchivevolume.md)

## Governing policies

- <a id="pa-8acb67e659"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.PackArchiveVolume.to_mapping`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 490483f64fa4895732a46bd80aa55b988d6ebceec60d9db978d3258cc502ce83 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "to_mapping",
  "owner": "riverhog_archive_contracts.PackArchiveVolume",
  "unit": "member"
}
```
