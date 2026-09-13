# riverhog_protocol.ProcessingClaimDocument.validate_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimdocument-ec04877d27:d9b4e1fc47 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-84e2cd5ea7"></a>
| Field | Shape |
|---|---|
| <a id="s-00e0f0b9a2"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-56db6a3dcd"></a>`distribution` | "riverhog-protocol" |
| <a id="s-7ab734df2e"></a>`module` | "riverhog_protocol" |
| <a id="s-00a53f00a9"></a>`name` | "validate_claim" |
| <a id="s-92310704a3"></a>`owner` | "riverhog_protocol.ProcessingClaimDocument" |
| <a id="s-0983eca8f4"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.ProcessingClaimDocument](riverhog-protocol-processingclaimdocument.md)

## Governing policies

- <a id="pa-de225973cd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimDocument.validate_claim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6d66998488a628d4ffaea49e123bc22118293c4bc659cacbd587ed38a1cea3b8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_claim",
  "owner": "riverhog_protocol.ProcessingClaimDocument",
  "unit": "member"
}
```
