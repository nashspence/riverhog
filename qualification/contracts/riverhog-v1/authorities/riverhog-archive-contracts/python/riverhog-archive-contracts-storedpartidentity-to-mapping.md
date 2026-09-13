# riverhog_archive_contracts.StoredPartIdentity.to_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-storedpartiden-343f09c92f:7115141fc9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-863850c43e"></a>
| Field | Shape |
|---|---|
| <a id="s-ebf1277da6"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-505b8bea99"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-ada6ef741c"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-a2325d11a4"></a>`name` | "to_mapping" |
| <a id="s-dc670f49dc"></a>`owner` | "riverhog_archive_contracts.StoredPartIdentity" |
| <a id="s-1fd16f5302"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_archive_contracts.StoredPartIdentity](riverhog-archive-contracts-storedpartidentity.md)

## Governing policies

- <a id="pa-e43591d127"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.StoredPartIdentity.to_mapping`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a53bd791374caad980412362d66bbe94c4162fec1002686ff397702dd95c1529 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "to_mapping",
  "owner": "riverhog_archive_contracts.StoredPartIdentity",
  "unit": "member"
}
```
