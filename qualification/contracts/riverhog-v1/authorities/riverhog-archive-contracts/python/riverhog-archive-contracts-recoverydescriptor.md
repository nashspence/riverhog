# riverhog_archive_contracts.RecoveryDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-recoverydescriptor:9f9198928e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d67095667b"></a>
- <a id="s-e0278487f3"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-7c9daf6e77"></a>`module`: `riverhog_archive_contracts`
- <a id="s-b52f9fda9f"></a>`name`: `RecoveryDescriptor`
- <a id="s-a8c51a83ec"></a>`unit`: `export`

### Declared structure

- <a id="s-68eb69d9af"></a>`kind`: `"class"`
- <a id="s-8e67e400cc"></a>`signature`: `"\"(encryption: 'CollectionEncryptionBinding', root: 'ArchiveRootCiphertextIdentity', schema: 'str' = 'riverhog-recovery-descriptor/v1') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-0613215bfc"></a>`encryption` | `'CollectionEncryptionBinding'` | `required` |
| <a id="s-8f752a2801"></a>`root` | `'ArchiveRootCiphertextIdentity'` | `required` |
| <a id="s-2e44a13a84"></a>`schema` | `'str'` | `'riverhog-recovery-descriptor/v1'` |

## Maintained corroboration

### Related interface records

- [to_json_bytes](riverhog-archive-contracts-recoverydescriptor-to-json-bytes.md)
- [from_json_bytes](riverhog-archive-contracts-recoverydescriptor-from-json-bytes.md)

## Governing policies

- <a id="pa-37a4e45efb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.RecoveryDescriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90e27d73e7656240e6da6becd56f94bbcf477f0ed0d06ed95403fa950dc15d3f -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "encryption",
        "type": "'CollectionEncryptionBinding'"
      },
      {
        "default": "required",
        "name": "root",
        "type": "'ArchiveRootCiphertextIdentity'"
      },
      {
        "default": "'riverhog-recovery-descriptor/v1'",
        "name": "schema",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(encryption: 'CollectionEncryptionBinding', root: 'ArchiveRootCiphertextIdentity', schema: 'str' = 'riverhog-recovery-descriptor/v1') -> None\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "RecoveryDescriptor",
  "unit": "export"
}
```

</details>
