# stove0_core.database_url_from_config

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-database-url-from-config:9cc9d6ae76 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3c99fd026a"></a>
- <a id="s-5c79cb299c"></a>`distribution`: `stove0-server`
- <a id="s-cc0468066e"></a>`module`: `stove0_core`
- <a id="s-072bf2a94f"></a>`name`: `database_url_from_config`
- <a id="s-a2762488c1"></a>`unit`: `export`

### Declared structure

- <a id="s-533b5295e3"></a>`kind`: `"function"`
- <a id="s-357d70da16"></a>`signature`: `"\"(path: 'Path \| None' = None) -> 'str'\""`

## Governing policies

- <a id="pa-3c42a374d1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.database_url_from_config`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 86aab25572d95be745292d5519cfcf8434439c5cd625f75b3f82a91db677dc31 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'Path | None' = None) -> 'str'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "database_url_from_config",
  "unit": "export"
}
```

</details>
