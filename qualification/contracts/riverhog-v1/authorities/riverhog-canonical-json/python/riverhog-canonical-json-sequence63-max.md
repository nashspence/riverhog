# riverhog_canonical_json.SEQUENCE63_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-canonical-json:riverhog-canonical-json-sequence63-max:7c73411bd1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-canonical-json](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-db9535c789"></a>
- <a id="s-ddafb35710"></a>`distribution`: `riverhog-canonical-json`
- <a id="s-8b1e311afa"></a>`module`: `riverhog_canonical_json`
- <a id="s-eca14972da"></a>`name`: `SEQUENCE63_MAX`
- <a id="s-135684b8b7"></a>`unit`: `export`

### Declared structure

- <a id="s-995cb1f09c"></a>`kind`: `"constant"`
- <a id="s-51e8c257c0"></a>`value`: `9223372036854775807`

## Governing policies

- <a id="pa-8e32773e00"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-canonical-json:riverhog_canonical_json](../../../evidence/sources/authorities.md#src-30f59d1ecd) — [packages/riverhog-canonical-json/src/riverhog\_canonical\_json/\_\_init\_\_.py](../../../../../../packages/riverhog-canonical-json/src/riverhog_canonical_json/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_canonical_json.SEQUENCE63_MAX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

Large integers appear as decimal strings in this machine representation. The machine artifact's `projection_unsafe_integer_paths` identifies them; primary content displays the recovered numeric values.

<!-- exact-contract-value: b03e27c44b1dc74235c0b2c29796491622f8ae82bff3363bc0f9af0095700617 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "9223372036854775807"
  },
  "distribution": "riverhog-canonical-json",
  "module": "riverhog_canonical_json",
  "name": "SEQUENCE63_MAX",
  "unit": "export"
}
```

</details>
