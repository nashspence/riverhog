# riverhog_archive_contracts.ProvenanceRootIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-provenancerootidentity:e4efc96f3b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-17d6d8f96d"></a>
- <a id="s-2b184b6f67"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-26a67fcd8a"></a>`module`: `riverhog_archive_contracts`
- <a id="s-0071f827a9"></a>`name`: `ProvenanceRootIdentity`
- <a id="s-c6f1270655"></a>`unit`: `export`

### Declared structure

- <a id="s-985fd207e9"></a>`kind`: `"class"`
- <a id="s-72f916512c"></a>`signature`: `"'(id: \\'str\\', kind: \"Literal[\\'provenance-root\\']\", path: \\'str\\', plaintext_bytes: \\'int\\', sha256: \\'str\\', stored_bytes: \\'int\\', stored_sha256: \\'str\\') -> None'"`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-906d88338e"></a>`id` | `'str'` | `required` |
| <a id="s-6c98ab8319"></a>`kind` | `"Literal['provenance-root']"` | `required` |
| <a id="s-4fd020fb6c"></a>`path` | `'str'` | `required` |
| <a id="s-a139df1f48"></a>`plaintext_bytes` | `'int'` | `required` |
| <a id="s-2a67424d38"></a>`sha256` | `'str'` | `required` |
| <a id="s-eb4ec1edf3"></a>`stored_bytes` | `'int'` | `required` |
| <a id="s-bd0efee06d"></a>`stored_sha256` | `'str'` | `required` |

## Maintained corroboration

### Related interface records

- [from_mapping](riverhog-archive-contracts-provenancerootidentity-from-mapping.md)
- [to_mapping](riverhog-archive-contracts-provenancerootidentity-to-mapping.md)

## Governing policies

- <a id="pa-d846ceed02"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.ProvenanceRootIdentity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8452bbb26fcc4d3e078ec3b81f092b9f71d942ef9124c5ac1c8d93ed6ebe7c2b -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "kind",
        "type": "\"Literal['provenance-root']\""
      },
      {
        "default": "required",
        "name": "path",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "plaintext_bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "stored_bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "stored_sha256",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "'(id: \\'str\\', kind: \"Literal[\\'provenance-root\\']\", path: \\'str\\', plaintext_bytes: \\'int\\', sha256: \\'str\\', stored_bytes: \\'int\\', stored_sha256: \\'str\\') -> None'"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "ProvenanceRootIdentity",
  "unit": "export"
}
```

</details>
