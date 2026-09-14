# riverhog_provenance.FileStateObserverFactory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-filestateobserverfactory:591bf1133e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9d962726d9"></a>
- <a id="s-fa26d65f49"></a>`distribution`: `riverhog-provenance`
- <a id="s-345a50bb38"></a>`module`: `riverhog_provenance`
- <a id="s-7ca9ec4d5d"></a>`name`: `FileStateObserverFactory`
- <a id="s-1affb467f4"></a>`unit`: `export`

### Declared structure

- <a id="s-8c66292337"></a>`kind`: `"type-alias"`
- <a id="s-57c8c4c40c"></a>`value`: `"collections.abc.Callable[[], riverhog_provenance.interface.FileStateObserver]"`

## Governing policies

- <a id="pa-418719e0a3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.FileStateObserverFactory`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc5e6fe4739f5b2c0d02a3b3d36d7907a8876c43e7fc476226411a75ea522223 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "collections.abc.Callable[[], riverhog_provenance.interface.FileStateObserver]"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "FileStateObserverFactory",
  "unit": "export"
}
```
