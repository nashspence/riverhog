# stove0_observer_protocol.SemanticValidatorProvider.resolve

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticvalidato-5a14b58164:ea262d2025 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6315005618"></a>
- <a id="s-12a53cf025"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-5585523f21"></a>`module`: `stove0_observer_protocol`
- <a id="s-3b112342d5"></a>`name`: `resolve`
- <a id="s-87289d5250"></a>`owner`: `stove0_observer_protocol.SemanticValidatorProvider`
- <a id="s-e25f4a2a28"></a>`unit`: `member`

### Declared structure

- <a id="s-4f77457130"></a>`kind`: `"method"`
- <a id="s-594bfa9cc4"></a>`signature`: `"\"(self, profile_id: 'str', profile_sha256: 'str') -> 'FactsSemanticValidator \| None'\""`

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.SemanticValidatorProvider](stove0-observer-protocol-semanticvalidatorprovider.md)

## Governing policies

- <a id="pa-23b174ff4f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticValidatorProvider.resolve`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 092b47744af308e3cc7a4f4d10a99c2c23cb63108ff614537bc5a5c435803946 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, profile_id: 'str', profile_sha256: 'str') -> 'FactsSemanticValidator | None'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "resolve",
  "owner": "stove0_observer_protocol.SemanticValidatorProvider",
  "unit": "member"
}
```
