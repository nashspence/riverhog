# riverhog_canonical_json.CanonicalJsonError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-canonical-json:riverhog-canonical-json-canonicaljsonerror:8f108b787a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-canonical-json](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2412f7bf23"></a>
- <a id="s-f93a69b30d"></a>`distribution`: `riverhog-canonical-json`
- <a id="s-d7784f6f71"></a>`module`: `riverhog_canonical_json`
- <a id="s-b6f71653b6"></a>`name`: `CanonicalJsonError`
- <a id="s-f2ecb258ff"></a>`unit`: `export`

### Declared structure

- <a id="s-2ea08f8455"></a>`kind`: `"class"`
- <a id="s-c82f948f41"></a>`signature`: `"\"(reason: 'str', detail: 'str') -> 'None'\""`

## Governing policies

- <a id="pa-3580e6b5eb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-canonical-json:riverhog_canonical_json](../../../evidence/sources/authorities.md#src-30f59d1ecd) — [packages/riverhog-canonical-json/src/riverhog\_canonical\_json/\_\_init\_\_.py](../../../../../../packages/riverhog-canonical-json/src/riverhog_canonical_json/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_canonical_json.CanonicalJsonError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1a1459647e4c916eadd873d047378705c9a172ff5f9d2770100216960e07a225 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(reason: 'str', detail: 'str') -> 'None'\""
  },
  "distribution": "riverhog-canonical-json",
  "module": "riverhog_canonical_json",
  "name": "CanonicalJsonError",
  "unit": "export"
}
```

</details>
