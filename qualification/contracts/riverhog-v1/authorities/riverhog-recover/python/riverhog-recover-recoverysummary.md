# riverhog_recover.RecoverySummary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-recover:riverhog-recover-recoverysummary:f16d383dbb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-recover](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9823f1528d"></a>
- <a id="s-a967310afa"></a>`distribution`: `riverhog-recover`
- <a id="s-b9db601456"></a>`module`: `riverhog_recover`
- <a id="s-c6edfe0d73"></a>`name`: `RecoverySummary`
- <a id="s-a251472e15"></a>`unit`: `export`

### Declared structure

- <a id="s-19da8f8947"></a>`kind`: `"class"`
- <a id="s-68b17cb4ba"></a>`signature`: `"\"(output: 'Path', files: 'int', bytes: 'int', volumes: 'int', provenance_mode: 'str' = 'omitted', provenance_journals: 'int' = 0) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-1f64fde4b2"></a>`output` | `'Path'` | `required` |
| <a id="s-c714379559"></a>`files` | `'int'` | `required` |
| <a id="s-d8ea05a84e"></a>`bytes` | `'int'` | `required` |
| <a id="s-c8b4cf4e48"></a>`volumes` | `'int'` | `required` |
| <a id="s-04f26cf34e"></a>`provenance_mode` | `'str'` | `'omitted'` |
| <a id="s-b086924dee"></a>`provenance_journals` | `'int'` | `0` |

## Governing policies

- <a id="pa-fa84cb4473"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-recover:riverhog_recover](../../../evidence/sources.md#src-dbfe6c5e2e) — `reference/riverhog/recovery/src/riverhog_recover/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_recover.RecoverySummary`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 168710aa9cb5d235bb8945bf9aa055b95726903c071ca4f2415f70aa00ec25f8 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "output",
        "type": "'Path'"
      },
      {
        "default": "required",
        "name": "files",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "volumes",
        "type": "'int'"
      },
      {
        "default": "'omitted'",
        "name": "provenance_mode",
        "type": "'str'"
      },
      {
        "default": "0",
        "name": "provenance_journals",
        "type": "'int'"
      }
    ],
    "kind": "class",
    "signature": "\"(output: 'Path', files: 'int', bytes: 'int', volumes: 'int', provenance_mode: 'str' = 'omitted', provenance_journals: 'int' = 0) -> None\""
  },
  "distribution": "riverhog-recover",
  "module": "riverhog_recover",
  "name": "RecoverySummary",
  "unit": "export"
}
```
