# riverhog_protocol.TransformIntent.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-transformintent-seal:bec91fb815 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4e2d80c3c1"></a>
- <a id="s-b18b208575"></a>`distribution`: `riverhog-protocol`
- <a id="s-bab0a16904"></a>`module`: `riverhog_protocol`
- <a id="s-8839218385"></a>`name`: `seal`
- <a id="s-d1e359579d"></a>`owner`: `riverhog_protocol.TransformIntent`
- <a id="s-4d6b1acbb5"></a>`unit`: `member`

### Declared structure

- <a id="s-b4594be164"></a>`kind`: `"classmethod"`
- <a id="s-ac1d0a6590"></a>`signature`: `"\"(cls, *, recipe: 'RecipeIdentity', operation: 'OperationIdentity', inputs: 'Sequence[CollectionRootIdentity]', effective_intent: 'Mapping[str, object]', retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0) -> 'TransformIntent'\""`

## Maintained corroboration

### Related interface records

- [TransformIntent](riverhog-protocol-transformintent.md)

## Governing policies

- <a id="pa-59e4bc5fa8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.TransformIntent.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 86f5f37782b2b764557594185e09dc762ee7de589d365ba3efc5215959f35d7e -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, recipe: 'RecipeIdentity', operation: 'OperationIdentity', inputs: 'Sequence[CollectionRootIdentity]', effective_intent: 'Mapping[str, object]', retirement_policy: 'RetirementPolicy' = 'retain', retirement_grace_seconds: 'int' = 0) -> 'TransformIntent'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "seal",
  "owner": "riverhog_protocol.TransformIntent",
  "unit": "member"
}
```
