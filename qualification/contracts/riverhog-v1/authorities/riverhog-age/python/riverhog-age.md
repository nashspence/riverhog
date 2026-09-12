# riverhog_age

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age:041d136a3a -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-age` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/python/2`

## Effective policies

- `compatibility/python-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:riverhog-age` — `packages/riverhog-age/src/riverhog_age/__init__.py::<module>`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract summary

| Field | Shape |
|---|---|
| `distribution` | "riverhog-age" |
| `exports` | object (21 fields) |
| `module` | "riverhog_age" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dbc94502693c9e96dd4ce1dcd315c9ee75212fffae9bad869d40587086ed0638 -->

```json
{
  "distribution": "riverhog-age",
  "exports": {
    "AEAD_TAG_SIZE": {
      "kind": "constant",
      "value": 16
    },
    "AgeAlignedUnitPlan": {
      "fields": [
        {
          "default": "required",
          "name": "unit_number",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "first_chunk",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "chunk_count",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "includes_age_prefix",
          "type": "'bool'"
        },
        {
          "default": "required",
          "name": "plaintext_start",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "plaintext_end",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "ciphertext_start",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "ciphertext_end",
          "type": "'int'"
        }
      ],
      "kind": "class",
      "members": {
        "ciphertext_len": {
          "kind": "property",
          "signature": "(self) -> 'int'"
        },
        "plaintext_len": {
          "kind": "property",
          "signature": "(self) -> 'int'"
        }
      },
      "signature": "(unit_number: 'int', first_chunk: 'int', chunk_count: 'int', includes_age_prefix: 'bool', plaintext_start: 'int', plaintext_end: 'int', ciphertext_start: 'int', ciphertext_end: 'int') -> None"
    },
    "AgeDecryptError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "AgeFormatError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "CHUNK_SIZE": {
      "kind": "constant",
      "value": 65536
    },
    "DEFAULT_CHUNKS_PER_AGE_UNIT": {
      "kind": "constant",
      "value": 1024
    },
    "DEFAULT_SCRYPT_LOG_N": {
      "kind": "constant",
      "value": 18
    },
    "PAYLOAD_NONCE_SIZE": {
      "kind": "constant",
      "value": 16
    },
    "ResumableAgeScryptSession": {
      "kind": "class",
      "members": {
        "age_aligned_unit_plans": {
          "kind": "method",
          "signature": "(self, plaintext_size: 'int', *, chunks_per_unit: 'int' = 1024) -> 'list[AgeAlignedUnitPlan]'"
        },
        "age_prefix": {
          "kind": "property",
          "signature": "(self) -> 'bytes'"
        },
        "create": {
          "kind": "classmethod",
          "signature": "(cls, passphrase: 'str | bytes', *, log_n: 'int' = 18, plaintext_size: 'int | None' = None, file_key: 'bytes | None' = None, scrypt_salt: 'bytes | None' = None, payload_nonce: 'bytes | None' = None, scrypt_maxmem: 'int | None' = None) -> 'ResumableAgeScryptSession'"
        },
        "decrypt_chunk": {
          "kind": "method",
          "signature": "(self, chunk_index: 'int', ciphertext: 'bytes', *, final: 'bool') -> 'bytes'"
        },
        "encrypt_chunk": {
          "kind": "method",
          "signature": "(self, chunk_index: 'int', plaintext: 'bytes', *, final: 'bool') -> 'bytes'"
        },
        "encrypt_part": {
          "kind": "method",
          "signature": "(self, plan: 'AgeAlignedUnitPlan', plaintext_chunk_provider: 'Callable[[int, int, int], bytes]', *, plaintext_size: 'int') -> 'bytes'"
        },
        "encrypt_plaintext": {
          "kind": "method",
          "signature": "(self, plaintext: 'bytes') -> 'bytes'"
        },
        "export_state": {
          "kind": "method",
          "signature": "(self, *, plaintext_size: 'int | None' = None) -> 'UploadState'"
        },
        "from_state": {
          "kind": "classmethod",
          "signature": "(cls, passphrase: 'str | bytes', state: 'UploadState | bytes | str | Mapping[str, object]', *, scrypt_maxmem: 'int | None' = None) -> 'ResumableAgeScryptSession'"
        }
      },
      "signature": "(*, header: 'bytes', payload_nonce: 'bytes', file_key: 'bytes')"
    },
    "UploadState": {
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
      "members": {
        "from_json_bytes": {
          "kind": "classmethod",
          "signature": "(cls, data: 'bytes | str') -> 'UploadState'"
        },
        "to_json_bytes": {
          "kind": "method",
          "signature": "(self) -> 'bytes'"
        }
      },
      "signature": "(header: 'bytes', payload_nonce: 'bytes', plaintext_size: 'int | None' = None, format: 'str' = 'age-v1-scrypt-resumable') -> None"
    },
    "age_chunk_count_for_plaintext_len": {
      "kind": "function",
      "signature": "(plaintext_size: 'int') -> 'int'"
    },
    "age_ciphertext_len_for_plaintext_len": {
      "kind": "function",
      "signature": "(plaintext_size: 'int', *, age_prefix_len: 'int') -> 'int'"
    },
    "decrypt_age_scrypt": {
      "kind": "function",
      "signature": "(age_file: 'bytes', passphrase: 'str | bytes', *, scrypt_maxmem: 'int | None' = None) -> 'bytes'"
    },
    "encrypt_age_scrypt": {
      "kind": "function",
      "signature": "(plaintext: 'bytes', passphrase: 'str | bytes', *, log_n: 'int' = 18, scrypt_maxmem: 'int | None' = None) -> 'bytes'"
    },
    "iter_decrypt_age_scrypt": {
      "kind": "function",
      "signature": "(chunks: 'Iterable[bytes]', passphrase: 'str | bytes', *, scrypt_maxmem: 'int | None' = None) -> 'Iterator[bytes]'"
    },
    "iter_decrypt_payload_chunks": {
      "kind": "function",
      "signature": "(file_key: 'bytes', payload_nonce: 'bytes', ciphertext_chunks: 'Iterable[bytes]') -> 'Iterator[bytes]'"
    },
    "make_age_aligned_unit_plans": {
      "kind": "function",
      "signature": "(plaintext_size: 'int', *, age_prefix_len: 'int', chunks_per_unit: 'int' = 1024) -> 'list[AgeAlignedUnitPlan]'"
    },
    "parse_scrypt_header": {
      "kind": "function",
      "signature": "(header: 'bytes') -> 'ParsedScryptHeader'"
    },
    "parse_scrypt_header_from_age_file": {
      "kind": "function",
      "signature": "(age_file: 'bytes') -> 'ParsedScryptHeader'"
    },
    "plaintext_bytes_for_ciphertext_offset": {
      "kind": "function",
      "signature": "(*, state: 'UploadState | bytes | str | Mapping[str, object]', plaintext_bytes: 'int', ciphertext_bytes: 'int', ciphertext_offset: 'int') -> 'int'"
    },
    "split_plaintext_chunks": {
      "kind": "function",
      "signature": "(plaintext_size: 'int') -> 'list[tuple[int, int, int, bool]]'"
    }
  },
  "module": "riverhog_age"
}
```
