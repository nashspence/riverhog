# riverhog_provenance.PROVENANCE_ROOT_DOCUMENT_BYTES_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-root-docum-ade87035c5:c3c205f1fa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c1bd1569b5"></a>
- <a id="s-51deb52572"></a>`distribution`: `riverhog-provenance`
- <a id="s-e56e19f2ff"></a>`module`: `riverhog_provenance`
- <a id="s-8da388ee51"></a>`name`: `PROVENANCE_ROOT_DOCUMENT_BYTES_MAX`
- <a id="s-092d7c09c1"></a>`unit`: `export`

### Declared structure

- <a id="s-d00f5c8e88"></a>`kind`: `"constant"`
- <a id="s-c65ac63864"></a>`value`: `65536`

## Governing policies

- <a id="pa-153101bcff"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_ROOT_DOCUMENT_BYTES_MAX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 35fcbfe7e209c78473b103902dcd659eee8223c1b5d0bf0e9f28bf9f26120b1f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 65536
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_ROOT_DOCUMENT_BYTES_MAX",
  "unit": "export"
}
```

</details>
