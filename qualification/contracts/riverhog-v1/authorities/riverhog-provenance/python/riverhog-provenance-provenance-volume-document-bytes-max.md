# riverhog_provenance.PROVENANCE_VOLUME_DOCUMENT_BYTES_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-volume-doc-fc5b967ccb:36e2e0bf15 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5a8c61e503"></a>
- <a id="s-bf036db9f5"></a>`distribution`: `riverhog-provenance`
- <a id="s-49b1bbc5e3"></a>`module`: `riverhog_provenance`
- <a id="s-7f857341b7"></a>`name`: `PROVENANCE_VOLUME_DOCUMENT_BYTES_MAX`
- <a id="s-87fe7e6132"></a>`unit`: `export`

### Declared structure

- <a id="s-8f830fe62c"></a>`kind`: `"constant"`
- <a id="s-98fb8144e2"></a>`value`: `65536`

## Governing policies

- <a id="pa-49d092ca42"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_VOLUME_DOCUMENT_BYTES_MAX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: da029a7351507af107514237b1f9bb3fd18c5671c2883c35c745e67bd3bbb4da -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 65536
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_VOLUME_DOCUMENT_BYTES_MAX",
  "unit": "export"
}
```

</details>
