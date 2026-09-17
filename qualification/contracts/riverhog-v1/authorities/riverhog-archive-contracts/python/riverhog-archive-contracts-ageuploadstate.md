# riverhog_archive_contracts.AgeUploadState

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-ageuploadstate:6325b17357 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b3fe433715"></a>
- <a id="s-a859710fcf"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-91afba18ad"></a>`module`: `riverhog_archive_contracts`
- <a id="s-6866536140"></a>`name`: `AgeUploadState`
- <a id="s-4471ba4655"></a>`unit`: `export`

### Declared structure

- <a id="s-635d06a186"></a>`kind`: `"class"`
- <a id="s-8fb7ae4b80"></a>`signature`: `"\"(header_b64: 'str', payload_nonce_b64: 'str', plaintext_size: 'int', format: 'str' = 'age-v1-scrypt-resumable') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-93e649becf"></a>`header_b64` | `'str'` | `required` |
| <a id="s-73abaafd44"></a>`payload_nonce_b64` | `'str'` | `required` |
| <a id="s-2ce2e66911"></a>`plaintext_size` | `'int'` | `required` |
| <a id="s-9cebff34a2"></a>`format` | `'str'` | `'age-v1-scrypt-resumable'` |

## Maintained corroboration

### Related interface records

- [from_mapping](riverhog-archive-contracts-ageuploadstate-from-mapping.md)
- [to_mapping](riverhog-archive-contracts-ageuploadstate-to-mapping.md)

## Governing policies

- <a id="pa-a9f9f156c1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.AgeUploadState`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 64b5a0ae8bb1d09c51798594ee612ee9c2cd97b2eb862df7f78c03fe37e535db -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "header_b64",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "payload_nonce_b64",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "plaintext_size",
        "type": "'int'"
      },
      {
        "default": "'age-v1-scrypt-resumable'",
        "name": "format",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(header_b64: 'str', payload_nonce_b64: 'str', plaintext_size: 'int', format: 'str' = 'age-v1-scrypt-resumable') -> None\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "AgeUploadState",
  "unit": "export"
}
```

</details>
