# stove0_protocol.SemanticValidationProfile.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-semanticvalidationprofile-seal:6ead8ac4a6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-95b590e634"></a>
| Field | Shape |
|---|---|
| <a id="s-c280d167e5"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-baf207f28b"></a>`distribution` | "stove0-protocol" |
| <a id="s-d71edc1405"></a>`module` | "stove0_protocol" |
| <a id="s-9193bf094e"></a>`name` | "seal" |
| <a id="s-5d9ce22bc6"></a>`owner` | "stove0_protocol.SemanticValidationProfile" |
| <a id="s-febdc859fd"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.SemanticValidationProfile](stove0-protocol-semanticvalidationprofile.md)

## Governing policies

- <a id="pa-c8a80d245e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.SemanticValidationProfile.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0fae39b785e047cc737e8b5895a6f06d147108bda386441cd6c633200b9c44ee -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'SemanticValidationProfilePayload') -> 'SemanticValidationProfile'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.SemanticValidationProfile",
  "unit": "member"
}
```
