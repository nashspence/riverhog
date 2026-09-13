# riverhog_age.decrypt_age_scrypt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-decrypt-age-scrypt:01c45bc8fd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cccefe0555"></a>
| Field | Shape |
|---|---|
| <a id="s-55f18209ec"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-e6a997f533"></a>`distribution` | "riverhog-age" |
| <a id="s-13a3e3e439"></a>`module` | "riverhog_age" |
| <a id="s-86ee678178"></a>`name` | "decrypt_age_scrypt" |
| <a id="s-5fdd3347b4"></a>`unit` | "export" |

## Governing policies

- <a id="pa-d03dc72852"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.decrypt_age_scrypt`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ca91731a5611935dc8953ac6fd027a38608b4aed64c8218517c4ebe2d3ff5b4b -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(age_file: 'bytes', passphrase: 'str | bytes', *, scrypt_maxmem: 'int | None' = None) -> 'bytes'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "decrypt_age_scrypt",
  "unit": "export"
}
```
