# riverhog_archive_contracts.ArchiveRootCiphertextIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-archiverootcip-293338e816:d315391ecd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf12943460"></a>
- <a id="s-eac426c881"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-6c52b98757"></a>`module`: `riverhog_archive_contracts`
- <a id="s-e3ab559411"></a>`name`: `ArchiveRootCiphertextIdentity`
- <a id="s-8e1760156e"></a>`unit`: `export`

### Declared structure

- <a id="s-174deb1e1e"></a>`kind`: `"class"`
- <a id="s-fbad04e9fd"></a>`signature`: `"\"(path: 'str', stored_bytes: 'int', stored_sha256: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-0df2e6cdbc"></a>`path` | `'str'` | `required` |
| <a id="s-601781cff9"></a>`stored_bytes` | `'int'` | `required` |
| <a id="s-7ebc9cbd82"></a>`stored_sha256` | `'str'` | `required` |

## Governing policies

- <a id="pa-07ecf3bba7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.ArchiveRootCiphertextIdentity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 73e85462b8eb3496839bef440e3bf9fef8a62e7bd82bfe69183d79f7458cd56f -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "path",
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
    "signature": "\"(path: 'str', stored_bytes: 'int', stored_sha256: 'str') -> None\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "ArchiveRootCiphertextIdentity",
  "unit": "export"
}
```

</details>
