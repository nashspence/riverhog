# riverhog_protocol.CollectionTagSet.identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagset-identity:5b89a92105 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e917edceea"></a>
| Field | Shape |
|---|---|
| <a id="s-c0ba2596fe"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-88c594fda0"></a>`distribution` | "riverhog-protocol" |
| <a id="s-bb21f5b7d6"></a>`module` | "riverhog_protocol" |
| <a id="s-3af566e9e8"></a>`name` | "identity" |
| <a id="s-580fd68a46"></a>`owner` | "riverhog_protocol.CollectionTagSet" |
| <a id="s-369635d29c"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionTagSet](riverhog-protocol-collectiontagset.md)

## Governing policies

- <a id="pa-bb999c1de9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagSet.identity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f9423ba7d56b4f3b94e7b3ad166c5897c6c7f4df9a50fbc24297d4ad793c712 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "identity",
  "owner": "riverhog_protocol.CollectionTagSet",
  "unit": "member"
}
```
