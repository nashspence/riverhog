# riverhog_protocol.ProvenanceSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-provenancesort:638bbbe1d4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0e0f3e5324"></a>
- <a id="s-849f688ed9"></a>`distribution`: `riverhog-protocol`
- <a id="s-701334d89b"></a>`module`: `riverhog_protocol`
- <a id="s-348f2cdc37"></a>`name`: `ProvenanceSort`
- <a id="s-caa97a4d63"></a>`unit`: `export`

### Declared structure

- <a id="s-7431f43a0a"></a>`kind`: `"type-alias"`
- <a id="s-ac135d9177"></a>`value`: `"typing.Literal['path', 'bytes', 'status']"`

## Governing policies

- <a id="pa-b8c82efd9a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProvenanceSort`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ca80ec9dc5b5d6c6f6b98d4dfe5fed37dce9c643d8e845c08f6e5ada42b8d56 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['path', 'bytes', 'status']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProvenanceSort",
  "unit": "export"
}
```
