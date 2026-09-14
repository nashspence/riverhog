# stove0_core.Stove0RuntimeConfig.from_environment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0runtimeconfig-from-environment:cf52de8088 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f63e4c88ac"></a>
- <a id="s-4a5d2657f5"></a>`distribution`: `stove0-server`
- <a id="s-cce9d7e581"></a>`module`: `stove0_core`
- <a id="s-231b4661cd"></a>`name`: `from_environment`
- <a id="s-d32e990a1f"></a>`owner`: `stove0_core.Stove0RuntimeConfig`
- <a id="s-5b1e129eea"></a>`unit`: `member`

### Declared structure

- <a id="s-cedbf4048f"></a>`kind`: `"classmethod"`
- <a id="s-0b1dfc68c1"></a>`signature`: `"\"(cls, environ: 'Mapping[str, str] \| None' = None, *, require_api_token: 'bool' = True) -> 'Stove0RuntimeConfig'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0RuntimeConfig](stove0-core-stove0runtimeconfig.md)

## Governing policies

- <a id="pa-9329effd9e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RuntimeConfig.from_environment`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6925352d961fe1788b1d1650a00da5409b194b3e35033ae7317ab207df0d790 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, environ: 'Mapping[str, str] | None' = None, *, require_api_token: 'bool' = True) -> 'Stove0RuntimeConfig'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "from_environment",
  "owner": "stove0_core.Stove0RuntimeConfig",
  "unit": "member"
}
```
