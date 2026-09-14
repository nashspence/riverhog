# riverhog_protocol.CollectionDerivation.to_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionderivation-to-json-bytes:bf1259ba66 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fb06f164e2"></a>
- <a id="s-dd0adfb813"></a>`distribution`: `riverhog-protocol`
- <a id="s-0bf75d24fe"></a>`module`: `riverhog_protocol`
- <a id="s-2ce74436a3"></a>`name`: `to_json_bytes`
- <a id="s-32940b58a2"></a>`owner`: `riverhog_protocol.CollectionDerivation`
- <a id="s-4fa4abf478"></a>`unit`: `member`

### Declared structure

- <a id="s-a02b70a9a0"></a>`kind`: `"method"`
- <a id="s-8b16597aea"></a>`signature`: `"\"(self) -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [CollectionDerivation](riverhog-protocol-collectionderivation.md)

## Governing policies

- <a id="pa-d1c772c8b7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDerivation.to_json_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d090acd778369a0bc06b60b4da2023e4dd9e4bfdce7a2a8bf44945997d3b890c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "to_json_bytes",
  "owner": "riverhog_protocol.CollectionDerivation",
  "unit": "member"
}
```
