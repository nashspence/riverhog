# riverhog_archive_contracts.PackArchiveVolume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-packarchivevolume:3090a7249b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7ba4643c35"></a>
- <a id="s-91e95c1a69"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-2237439c7c"></a>`module`: `riverhog_archive_contracts`
- <a id="s-b581a0f43d"></a>`name`: `PackArchiveVolume`
- <a id="s-05777ad172"></a>`unit`: `export`

### Declared structure

- <a id="s-7e688e6b95"></a>`kind`: `"class"`
- <a id="s-1ed2491f73"></a>`signature`: `"'(id: \\'str\\', sequence: \\'int\\', path: \\'str\\', files: \\'int\\', source_bytes: \\'int\\', plaintext_bytes: \\'int\\', age_state: \\'AgeUploadState\\', index_sha256: \\'str\\', plan_sha256: \\'str\\', parts: \\'tuple[StoredPartIdentity, ...]\\', kind: \"Literal[\\'pack\\']\" = \\'pack\\') -> None'"`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-a41a550698"></a>`id` | `'str'` | `required` |
| <a id="s-c8064eb622"></a>`sequence` | `'int'` | `required` |
| <a id="s-6ee9c7dd83"></a>`path` | `'str'` | `required` |
| <a id="s-8bcad68d63"></a>`files` | `'int'` | `required` |
| <a id="s-5208695108"></a>`source_bytes` | `'int'` | `required` |
| <a id="s-e6fe6e5946"></a>`plaintext_bytes` | `'int'` | `required` |
| <a id="s-2571f9d9e8"></a>`age_state` | `'AgeUploadState'` | `required` |
| <a id="s-92066040d3"></a>`index_sha256` | `'str'` | `required` |
| <a id="s-1e4fa51b7f"></a>`plan_sha256` | `'str'` | `required` |
| <a id="s-a722bcbd39"></a>`parts` | `'tuple[StoredPartIdentity, ...]'` | `required` |
| <a id="s-86da2977c0"></a>`kind` | `"Literal['pack']"` | `'pack'` |

## Maintained corroboration

### Related interface records

- [to_mapping](riverhog-archive-contracts-packarchivevolume-to-mapping.md)

## Governing policies

- <a id="pa-728bfab5e2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.PackArchiveVolume`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f17fa09699bf2484606e5654d8ed2948c1b223c8b7c1fc7d14faf1bc7d1aaf97 -->

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
        "name": "sequence",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "path",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "files",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "source_bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "plaintext_bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "age_state",
        "type": "'AgeUploadState'"
      },
      {
        "default": "required",
        "name": "index_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "plan_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "parts",
        "type": "'tuple[StoredPartIdentity, ...]'"
      },
      {
        "default": "'pack'",
        "name": "kind",
        "type": "\"Literal['pack']\""
      }
    ],
    "kind": "class",
    "signature": "'(id: \\'str\\', sequence: \\'int\\', path: \\'str\\', files: \\'int\\', source_bytes: \\'int\\', plaintext_bytes: \\'int\\', age_state: \\'AgeUploadState\\', index_sha256: \\'str\\', plan_sha256: \\'str\\', parts: \\'tuple[StoredPartIdentity, ...]\\', kind: \"Literal[\\'pack\\']\" = \\'pack\\') -> None'"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "PackArchiveVolume",
  "unit": "export"
}
```
