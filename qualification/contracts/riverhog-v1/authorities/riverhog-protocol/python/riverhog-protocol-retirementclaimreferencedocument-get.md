# riverhog_protocol.RetirementClaimReferenceDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retirementclaimreferenc-c613329ddf:7f4f57e9e6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-773206ea92"></a>
- <a id="s-15b8ff601d"></a>`distribution`: `riverhog-protocol`
- <a id="s-3f2d9b912d"></a>`module`: `riverhog_protocol`
- <a id="s-b2f030fcf4"></a>`name`: `get`
- <a id="s-0699280d16"></a>`owner`: `riverhog_protocol.RetirementClaimReferenceDocument`
- <a id="s-3bcd184750"></a>`unit`: `member`

### Declared structure

- <a id="s-48ce4d0eb3"></a>`kind`: `"method"`
- <a id="s-13df974ff0"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [RetirementClaimReferenceDocument](riverhog-protocol-retirementclaimreferencedocument.md)

## Governing policies

- <a id="pa-00961e1609"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RetirementClaimReferenceDocument.get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c95f02bd0f126e410f529e2b1c993b6204612d68d505bc7c1f46f49b1edbe0ea -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.RetirementClaimReferenceDocument",
  "unit": "member"
}
```
