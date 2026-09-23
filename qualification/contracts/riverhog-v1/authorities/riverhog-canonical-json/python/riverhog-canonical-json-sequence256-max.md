# riverhog_canonical_json.SEQUENCE256_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-canonical-json:riverhog-canonical-json-sequence256-max:9727d825aa -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-canonical-json](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-01d84375ef"></a>
- <a id="s-c8d80b3b77"></a>`distribution`: `riverhog-canonical-json`
- <a id="s-7551a38786"></a>`module`: `riverhog_canonical_json`
- <a id="s-1f116d38ac"></a>`name`: `SEQUENCE256_MAX`
- <a id="s-9c38309009"></a>`unit`: `export`

### Declared structure

- <a id="s-96489417de"></a>`kind`: `"constant"`
- <a id="s-e7d9812116"></a>`value`: `115792089237316195423570985008687907853269984665640564039457584007913129639935`

## Governing policies

- <a id="pa-ab5c67c775"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-canonical-json:riverhog_canonical_json](../../../evidence/sources/authorities.md#src-30f59d1ecd) — [packages/riverhog-canonical-json/src/riverhog\_canonical\_json/\_\_init\_\_.py](../../../../../../packages/riverhog-canonical-json/src/riverhog_canonical_json/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_canonical_json.SEQUENCE256_MAX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

Large integers appear as decimal strings in this machine representation. The machine artifact's `projection_unsafe_integer_paths` identifies them; primary content displays the recovered numeric values.

<!-- exact-contract-value: c994b408d553f91bee4c72ee18b5b9ecc93a3677dc379b8164106e98ceb97940 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "115792089237316195423570985008687907853269984665640564039457584007913129639935"
  },
  "distribution": "riverhog-canonical-json",
  "module": "riverhog_canonical_json",
  "name": "SEQUENCE256_MAX",
  "unit": "export"
}
```

</details>
