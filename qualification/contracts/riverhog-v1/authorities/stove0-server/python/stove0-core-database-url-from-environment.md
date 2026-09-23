# stove0_core.database_url_from_environment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-database-url-from-environment:517fb10ec9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bc1975d97d"></a>
- <a id="s-545e64e85b"></a>`distribution`: `stove0-server`
- <a id="s-cf6b6e0919"></a>`module`: `stove0_core`
- <a id="s-d3ee9368c7"></a>`name`: `database_url_from_environment`
- <a id="s-134123e837"></a>`unit`: `export`

### Declared structure

- <a id="s-22d450d4a4"></a>`kind`: `"function"`
- <a id="s-e2a031dc51"></a>`signature`: `"\"(environ: 'Mapping[str, str] \| None' = None) -> 'str'\""`

## Governing policies

- <a id="pa-5351da0f85"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.database_url_from_environment`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c8cc4ee8c43b365b7f0bb6f06ee379ac63213c540589db67430670712256e488 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(environ: 'Mapping[str, str] | None' = None) -> 'str'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "database_url_from_environment",
  "unit": "export"
}
```

</details>
