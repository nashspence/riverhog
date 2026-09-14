# riverhog_protocol.SortOrder

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-sortorder:09af378db4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-20d9dde584"></a>
- <a id="s-8d3f55e730"></a>`distribution`: `riverhog-protocol`
- <a id="s-546f7ffbbb"></a>`module`: `riverhog_protocol`
- <a id="s-4f7803c341"></a>`name`: `SortOrder`
- <a id="s-a567a595a0"></a>`unit`: `export`

### Declared structure

- <a id="s-d04ee107cd"></a>`kind`: `"type-alias"`
- <a id="s-2506fcbd26"></a>`value`: `"typing.Literal['asc', 'desc']"`

## Governing policies

- <a id="pa-23fbcfdadf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.SortOrder`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb1f8ae52a24d8187123cc3323b8cde0d54af813116f3d4964a08e4ba3d0c60f -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['asc', 'desc']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "SortOrder",
  "unit": "export"
}
```
