# riverhog_protocol.CollectionDerivation.as_dict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionderivation-as-dict:c836ba31e6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-67cf7fc70f"></a>
- <a id="s-323dabe59c"></a>`distribution`: `riverhog-protocol`
- <a id="s-a07c906b60"></a>`module`: `riverhog_protocol`
- <a id="s-b834f154a4"></a>`name`: `as_dict`
- <a id="s-26cf87baff"></a>`owner`: `riverhog_protocol.CollectionDerivation`
- <a id="s-3834ea3d4b"></a>`unit`: `member`

### Declared structure

- <a id="s-3aca958e3f"></a>`kind`: `"method"`
- <a id="s-d12b35e465"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [CollectionDerivation](riverhog-protocol-collectionderivation.md)

## Governing policies

- <a id="pa-e1d3ce75a7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDerivation.as_dict`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4628ba51a9858a2d95f3080951c0cd42efe009692933f53dfab739bbf6497420 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "as_dict",
  "owner": "riverhog_protocol.CollectionDerivation",
  "unit": "member"
}
```
