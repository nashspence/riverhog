# riverhog_age.parse_scrypt_header_from_age_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-parse-scrypt-header-from-age-file:e921d72dc6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6a710c7ccd"></a>
| Field | Shape |
|---|---|
| <a id="s-e66fc6faa2"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d36a71d0e1"></a>`distribution` | "riverhog-age" |
| <a id="s-7177fdb65c"></a>`module` | "riverhog_age" |
| <a id="s-60335a0672"></a>`name` | "parse_scrypt_header_from_age_file" |
| <a id="s-5c21d08bbe"></a>`unit` | "export" |

## Governing policies

- <a id="pa-8d1428df44"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.parse_scrypt_header_from_age_file`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 010f7a46c10f5cdb9805ca0dede23f38956c55190326aab455f2a9956a266802 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(age_file: 'bytes') -> 'ParsedScryptHeader'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "parse_scrypt_header_from_age_file",
  "unit": "export"
}
```
