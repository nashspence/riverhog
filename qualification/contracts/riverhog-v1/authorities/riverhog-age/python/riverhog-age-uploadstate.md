# riverhog_age.UploadState

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-uploadstate:806e0073a8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f7e65dc723"></a>
- <a id="s-6e78f52c70"></a>`distribution`: `riverhog-age`
- <a id="s-06614783f8"></a>`module`: `riverhog_age`
- <a id="s-e67255d16b"></a>`name`: `UploadState`
- <a id="s-d36a0c0574"></a>`unit`: `export`

### Declared structure

- <a id="s-04ac7dba43"></a>`kind`: `"class"`
- <a id="s-09da5e35d7"></a>`signature`: `"\"(header: 'bytes', payload_nonce: 'bytes', plaintext_size: 'int \| None' = None, format: 'str' = 'age-v1-scrypt-resumable') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-f9b3b77d98"></a>`header` | `'bytes'` | `required` |
| <a id="s-ec22247ecc"></a>`payload_nonce` | `'bytes'` | `required` |
| <a id="s-91d237cdd3"></a>`plaintext_size` | `'int \| None'` | `None` |
| <a id="s-f8c6e2d2dc"></a>`format` | `'str'` | `'age-v1-scrypt-resumable'` |

## Maintained corroboration

### Related interface records

- [from_json_bytes](riverhog-age-uploadstate-from-json-bytes.md)
- [to_json_bytes](riverhog-age-uploadstate-to-json-bytes.md)

## Governing policies

- <a id="pa-11626d8fa6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources/authorities.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.UploadState`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
