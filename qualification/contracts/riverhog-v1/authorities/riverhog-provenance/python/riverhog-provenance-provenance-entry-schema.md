# riverhog_provenance.PROVENANCE_ENTRY_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-entry-schema:1631aa6e2f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0eb5a276f3"></a>
- <a id="s-57ab225a6e"></a>`distribution`: `riverhog-provenance`
- <a id="s-ebe22ce997"></a>`module`: `riverhog_provenance`
- <a id="s-59fb6d252c"></a>`name`: `PROVENANCE_ENTRY_SCHEMA`
- <a id="s-ab11138b82"></a>`unit`: `export`

### Declared structure

- <a id="s-7da3d3f8c2"></a>`kind`: `"constant"`
- <a id="s-2435528f52"></a>`value`: `"https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json"`

## Governing policies

- <a id="pa-22ce7f2446"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_ENTRY_SCHEMA`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd106e7ddbafe91f68691d489e4c5f8f59f5860e38d199d74cd6c469c3c20464 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_ENTRY_SCHEMA",
  "unit": "export"
}
```

</details>
