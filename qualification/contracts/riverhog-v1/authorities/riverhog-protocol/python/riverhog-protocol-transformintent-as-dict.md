# riverhog_protocol.TransformIntent.as_dict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-transformintent-as-dict:4e3173c23c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4c5edaf937"></a>
| Field | Shape |
|---|---|
| <a id="s-12ceb87209"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d727febd90"></a>`distribution` | "riverhog-protocol" |
| <a id="s-88a762786a"></a>`module` | "riverhog_protocol" |
| <a id="s-5b9ab3cc1b"></a>`name` | "as_dict" |
| <a id="s-e36d66382f"></a>`owner` | "riverhog_protocol.TransformIntent" |
| <a id="s-f019051657"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.TransformIntent](riverhog-protocol-transformintent.md)

## Governing policies

- <a id="pa-83d62d5622"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.TransformIntent.as_dict`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d2075981e36a8814ada4908624a6c66bbb1ce74167ee6e3fd24642ace2f534d4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "as_dict",
  "owner": "riverhog_protocol.TransformIntent",
  "unit": "member"
}
```
