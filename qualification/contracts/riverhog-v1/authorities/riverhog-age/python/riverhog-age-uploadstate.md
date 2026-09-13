# riverhog_age.UploadState

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-uploadstate:806e0073a8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f7e65dc723"></a>
| Field | Shape |
|---|---|
| <a id="s-17d140fe82"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-6e78f52c70"></a>`distribution` | "riverhog-age" |
| <a id="s-06614783f8"></a>`module` | "riverhog_age" |
| <a id="s-e67255d16b"></a>`name` | "UploadState" |
| <a id="s-d36a0c0574"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_age.UploadState.from_json_bytes](riverhog-age-uploadstate-from-json-bytes.md)
- [riverhog_age.UploadState.to_json_bytes](riverhog-age-uploadstate-to-json-bytes.md)

## Governing policies

- <a id="pa-11626d8fa6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.UploadState`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bfc54c09b1c237bcc57153f346a6ecce43ee51e64524ec30bc4bad6e48e7d654 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "header",
        "type": "'bytes'"
      },
      {
        "default": "required",
        "name": "payload_nonce",
        "type": "'bytes'"
      },
      {
        "default": "None",
        "name": "plaintext_size",
        "type": "'int | None'"
      },
      {
        "default": "'age-v1-scrypt-resumable'",
        "name": "format",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(header: 'bytes', payload_nonce: 'bytes', plaintext_size: 'int | None' = None, format: 'str' = 'age-v1-scrypt-resumable') -> None\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "UploadState",
  "unit": "export"
}
```
