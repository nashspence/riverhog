# riverhog_provenance.ProvenancePayloadIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenancepayloadidentity:7ce02f2917 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a2e1505ad2"></a>
- <a id="s-e6fee813dc"></a>`distribution`: `riverhog-provenance`
- <a id="s-b88ae841f9"></a>`module`: `riverhog_provenance`
- <a id="s-63454acb2e"></a>`name`: `ProvenancePayloadIdentity`
- <a id="s-bc2d19cc91"></a>`unit`: `export`

### Declared structure

- <a id="s-74d760ca2c"></a>`kind`: `"class"`
- <a id="s-82bdec24a3"></a>`signature`: `"'(kind: \"Literal[\\'bindings\\', \\'journal\\']\", path: \\'str\\', bytes: \\'int\\', sha256: \\'str\\') -> None'"`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-4c40c1650e"></a>`kind` | `"Literal['bindings', 'journal']"` | `required` |
| <a id="s-1efe372afe"></a>`path` | `'str'` | `required` |
| <a id="s-5f7c8ae54e"></a>`bytes` | `'int'` | `required` |
| <a id="s-b3a4095d28"></a>`sha256` | `'str'` | `required` |

## Maintained corroboration

### Related interface records

- [to_mapping](riverhog-provenance-provenancepayloadidentity-to-mapping.md)

## Governing policies

- <a id="pa-cb19439007"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenancePayloadIdentity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 936c7ccce0748904dcb35d157d637915fa8b474921f5e4b02a439757b7841051 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "kind",
        "type": "\"Literal['bindings', 'journal']\""
      },
      {
        "default": "required",
        "name": "path",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "sha256",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "'(kind: \"Literal[\\'bindings\\', \\'journal\\']\", path: \\'str\\', bytes: \\'int\\', sha256: \\'str\\') -> None'"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "ProvenancePayloadIdentity",
  "unit": "export"
}
```

</details>
