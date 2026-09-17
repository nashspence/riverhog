# riverhog_provenance.ProvenanceProviderMetadata

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenanceprovidermetadata:6ba4d1b9cc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5db6f18e79"></a>
- <a id="s-da9919583a"></a>`distribution`: `riverhog-provenance`
- <a id="s-35341a4a10"></a>`module`: `riverhog_provenance`
- <a id="s-358e376a9f"></a>`name`: `ProvenanceProviderMetadata`
- <a id="s-4c5c6e0b2f"></a>`unit`: `export`

### Declared structure

- <a id="s-0cf5d7c081"></a>`kind`: `"class"`
- <a id="s-ef227ab955"></a>`signature`: `"\"(name: 'str', value: 'str', distribution: 'str \| None', version: 'str \| None') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-7ec5cadf44"></a>`name` | `'str'` | `required` |
| <a id="s-c090ac3feb"></a>`value` | `'str'` | `required` |
| <a id="s-c1dc616d05"></a>`distribution` | `'str \| None'` | `required` |
| <a id="s-680ec8680a"></a>`version` | `'str \| None'` | `required` |

## Maintained corroboration

### Related interface records

- [as_dict](riverhog-provenance-provenanceprovidermetadata-as-dict.md)

## Governing policies

- <a id="pa-104be5bd15"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceProviderMetadata`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5c8cfda4c01091f6410026e9753b0ef9e5ab273c1e41221c295c6cd5ef6c9f75 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "name",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "value",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "distribution",
        "type": "'str | None'"
      },
      {
        "default": "required",
        "name": "version",
        "type": "'str | None'"
      }
    ],
    "kind": "class",
    "signature": "\"(name: 'str', value: 'str', distribution: 'str | None', version: 'str | None') -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "ProvenanceProviderMetadata",
  "unit": "export"
}
```

</details>
