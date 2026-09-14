# riverhog_age.AgeAlignedUnitPlan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-agealignedunitplan:1c79305317 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8ea299d5e8"></a>
- <a id="s-038046d8ba"></a>`distribution`: `riverhog-age`
- <a id="s-736aeeeb6e"></a>`module`: `riverhog_age`
- <a id="s-3085907738"></a>`name`: `AgeAlignedUnitPlan`
- <a id="s-74294cdbac"></a>`unit`: `export`

### Declared structure

- <a id="s-a6924ec6a5"></a>`kind`: `"class"`
- <a id="s-98e4c44948"></a>`signature`: `"\"(unit_number: 'int', first_chunk: 'int', chunk_count: 'int', includes_age_prefix: 'bool', plaintext_start: 'int', plaintext_end: 'int', ciphertext_start: 'int', ciphertext_end: 'int') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-1029dc0b45"></a>`unit_number` | `'int'` | `required` |
| <a id="s-d2425e5be6"></a>`first_chunk` | `'int'` | `required` |
| <a id="s-74cfff65f5"></a>`chunk_count` | `'int'` | `required` |
| <a id="s-8190206e18"></a>`includes_age_prefix` | `'bool'` | `required` |
| <a id="s-764a60a05c"></a>`plaintext_start` | `'int'` | `required` |
| <a id="s-09c7351d0b"></a>`plaintext_end` | `'int'` | `required` |
| <a id="s-d98e0a3de4"></a>`ciphertext_start` | `'int'` | `required` |
| <a id="s-f1080fd1b3"></a>`ciphertext_end` | `'int'` | `required` |

## Maintained corroboration

### Related interface records

- [riverhog_age.AgeAlignedUnitPlan.ciphertext_len](riverhog-age-agealignedunitplan-ciphertext-len.md)
- [riverhog_age.AgeAlignedUnitPlan.plaintext_len](riverhog-age-agealignedunitplan-plaintext-len.md)

## Governing policies

- <a id="pa-01c09d818b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.AgeAlignedUnitPlan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7f79233100b2b19389d1282c0fa1b675b9f1f61d0a913f4d3880cdcdafd47454 -->

```json
{
  "contract": {
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
    "signature": "\"(unit_number: 'int', first_chunk: 'int', chunk_count: 'int', includes_age_prefix: 'bool', plaintext_start: 'int', plaintext_end: 'int', ciphertext_start: 'int', ciphertext_end: 'int') -> None\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "AgeAlignedUnitPlan",
  "unit": "export"
}
```
