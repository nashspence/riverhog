# riverhog_protocol.PortableCollectionInventoryPage.validate_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-portablecollectioninven-540a77e796:922be6bc49 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-75d3a199d7"></a>
| Field | Shape |
|---|---|
| <a id="s-b1cdbbedfc"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-70184ab45e"></a>`distribution` | "riverhog-protocol" |
| <a id="s-b7c0af9e93"></a>`module` | "riverhog_protocol" |
| <a id="s-3002a1af73"></a>`name` | "validate_page" |
| <a id="s-91bf33d5bd"></a>`owner` | "riverhog_protocol.PortableCollectionInventoryPage" |
| <a id="s-15ee36fead"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.PortableCollectionInventoryPage](riverhog-protocol-portablecollectioninventorypage.md)

## Governing policies

- <a id="pa-2a97a36159"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.PortableCollectionInventoryPage.validate_page`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f9688b1264a821fc4ac3e4dc6966576a30e5c7dbe7e6f22bc44e221192e875d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'PortableCollectionInventoryPage'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_page",
  "owner": "riverhog_protocol.PortableCollectionInventoryPage",
  "unit": "member"
}
```
