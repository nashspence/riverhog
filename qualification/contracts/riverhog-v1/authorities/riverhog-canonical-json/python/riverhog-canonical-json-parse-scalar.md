# riverhog_canonical_json.parse_scalar

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-canonical-json:riverhog-canonical-json-parse-scalar:5e1f33ff29 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-canonical-json](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d867270aa0"></a>
- <a id="s-877e052c5c"></a>`distribution`: `riverhog-canonical-json`
- <a id="s-b4697ad528"></a>`module`: `riverhog_canonical_json`
- <a id="s-c1a9938409"></a>`name`: `parse_scalar`
- <a id="s-6c8e42833a"></a>`unit`: `export`

### Declared structure

- <a id="s-3d49d96629"></a>`kind`: `"function"`
- <a id="s-6d446237cb"></a>`signature`: `"\"(domain: 'ScalarDomain', value: 'object') -> 'int'\""`

## Governing policies

- <a id="pa-d0c977d449"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-canonical-json:riverhog_canonical_json](../../../evidence/sources/authorities.md#src-30f59d1ecd) — [packages/riverhog-canonical-json/src/riverhog\_canonical\_json/\_\_init\_\_.py](../../../../../../packages/riverhog-canonical-json/src/riverhog_canonical_json/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_canonical_json.parse_scalar`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f690b9c3c72c72a3af1ac0f15a400e22020b073ce7e965350afde255daa392f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(domain: 'ScalarDomain', value: 'object') -> 'int'\""
  },
  "distribution": "riverhog-canonical-json",
  "module": "riverhog_canonical_json",
  "name": "parse_scalar",
  "unit": "export"
}
```

</details>
