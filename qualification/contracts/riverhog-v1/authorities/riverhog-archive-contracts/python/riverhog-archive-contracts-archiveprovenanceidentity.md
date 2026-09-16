# riverhog_archive_contracts.ArchiveProvenanceIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-archiveprovenanceidentity:b52208478f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2469377a32"></a>
- <a id="s-f8b9f1bffb"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-5903669145"></a>`module`: `riverhog_archive_contracts`
- <a id="s-8bcbded1c2"></a>`name`: `ArchiveProvenanceIdentity`
- <a id="s-02e371ce40"></a>`unit`: `export`

### Declared structure

- <a id="s-2bdde06115"></a>`kind`: `"class"`
- <a id="s-e16b148b28"></a>`signature`: `"\"(identity: 'str', root: 'ProvenanceRootIdentity') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-fc59b16b96"></a>`identity` | `'str'` | `required` |
| <a id="s-2fa234dd81"></a>`root` | `'ProvenanceRootIdentity'` | `required` |

## Maintained corroboration

### Related interface records

- [to_mapping](riverhog-archive-contracts-archiveprovenanceidentity-to-mapping.md)
- [from_mapping](riverhog-archive-contracts-archiveprovenanceidentity-from-mapping.md)

## Governing policies

- <a id="pa-6a9f600f2f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.ArchiveProvenanceIdentity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9e50fa54e94f23114c1605a5bd72dc77ff003fb37cc4c0c486924032ae3e2ca9 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "identity",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "root",
        "type": "'ProvenanceRootIdentity'"
      }
    ],
    "kind": "class",
    "signature": "\"(identity: 'str', root: 'ProvenanceRootIdentity') -> None\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "ArchiveProvenanceIdentity",
  "unit": "export"
}
```

</details>
