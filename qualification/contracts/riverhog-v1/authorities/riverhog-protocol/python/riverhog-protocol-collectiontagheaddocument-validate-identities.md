# riverhog_protocol.CollectionTagHeadDocument.validate_identities

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagheaddocume-196fe21dea:f60cd779f2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-41917625b7"></a>
| Field | Shape |
|---|---|
| <a id="s-498f551f9d"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d11ab6b0ef"></a>`distribution` | "riverhog-protocol" |
| <a id="s-ae1728b717"></a>`module` | "riverhog_protocol" |
| <a id="s-501800ccc8"></a>`name` | "validate_identities" |
| <a id="s-8e7ff14d74"></a>`owner` | "riverhog_protocol.CollectionTagHeadDocument" |
| <a id="s-3abfb790ae"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionTagHeadDocument](riverhog-protocol-collectiontagheaddocument.md)

## Governing policies

- <a id="pa-8f707c89cb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagHeadDocument.validate_identities`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4ee27e6f3572f8b744a409938040c90f7172ce5730c8a847ec29e252bb4dda65 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_identities",
  "owner": "riverhog_protocol.CollectionTagHeadDocument",
  "unit": "member"
}
```
