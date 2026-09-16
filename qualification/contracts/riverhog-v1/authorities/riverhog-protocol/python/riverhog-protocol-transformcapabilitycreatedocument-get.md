# riverhog_protocol.TransformCapabilityCreateDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-transformcapabilitycrea-b25c7924bd:a309329ad5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-43b1a410fc"></a>
- <a id="s-c9336e9706"></a>`distribution`: `riverhog-protocol`
- <a id="s-10ae9c6e29"></a>`module`: `riverhog_protocol`
- <a id="s-07e33b2600"></a>`name`: `get`
- <a id="s-36f621d816"></a>`owner`: `riverhog_protocol.TransformCapabilityCreateDocument`
- <a id="s-3df69ce298"></a>`unit`: `member`

### Declared structure

- <a id="s-497512dc2a"></a>`kind`: `"method"`
- <a id="s-2313fbd53b"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [TransformCapabilityCreateDocument](riverhog-protocol-transformcapabilitycreatedocument.md)

## Governing policies

- <a id="pa-36198a7e6b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.TransformCapabilityCreateDocument.get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59632cbccf1b8b95e2afea19b9e0777f358c578c47b8856a97c99b90866cfc11 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.TransformCapabilityCreateDocument",
  "unit": "member"
}
```
