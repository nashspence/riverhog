# stove0_core.Stove0RiverhogClient.begin_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-begin-retirement:1bde7524a9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-77613300e3"></a>
- <a id="s-967d1b9f41"></a>`distribution`: `stove0-server`
- <a id="s-c7b55e9d31"></a>`module`: `stove0_core`
- <a id="s-79b72ede2b"></a>`name`: `begin_retirement`
- <a id="s-3ae2cf9aaf"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-106b5f79dd"></a>`unit`: `member`

### Declared structure

- <a id="s-4b0038ea87"></a>`kind`: `"method"`
- <a id="s-0dbb6160f3"></a>`signature`: `"\"(self, record: 'WorkRecord') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-67b70525c4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.begin_retirement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5023c9880ea3e1d3bbffb6b16d51d1b7affb9e1f6a7868dcb505279f66d6225d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord') -> 'bool'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "begin_retirement",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```

</details>
