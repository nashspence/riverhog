# a_riverhog_recovery_tool.RecoverySummary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-recovery-tool:a-riverhog-recovery-tool-recoverysummary:5daabf1765 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-recovery-tool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ee7491889b"></a>
- <a id="s-fcc3c58932"></a>`distribution`: `a-riverhog-recovery-tool`
- <a id="s-93f16744e4"></a>`module`: `a_riverhog_recovery_tool`
- <a id="s-59532fe847"></a>`name`: `RecoverySummary`
- <a id="s-435b9ae880"></a>`unit`: `export`

### Declared structure

- <a id="s-357deb955b"></a>`kind`: `"class"`
- <a id="s-ddcb2697f0"></a>`signature`: `"\"(output: 'Path', files: 'int', bytes: 'int', volumes: 'int', provenance_mode: 'str' = 'omitted', provenance_journals: 'int' = 0) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-9e59b8ce03"></a>`output` | `'Path'` | `required` |
| <a id="s-7fda8eb6c7"></a>`files` | `'int'` | `required` |
| <a id="s-7e78a80bd5"></a>`bytes` | `'int'` | `required` |
| <a id="s-438eefe60b"></a>`volumes` | `'int'` | `required` |
| <a id="s-666a11f57e"></a>`provenance_mode` | `'str'` | `'omitted'` |
| <a id="s-2c33a67b4b"></a>`provenance_journals` | `'int'` | `0` |

## Governing policies

- <a id="pa-985d52b19f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-recovery-tool:a_riverhog_recovery_tool](../../../evidence/sources/authorities.md#src-a085f6d973) — [some-implementations/riverhog/recovery/src/a\_riverhog\_recovery\_tool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_recovery_tool.RecoverySummary`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e56202b1de02f6bdd62ad216fc868e55bbf7dd59897f004b6246edf539518d65 -->

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
  "distribution": "a-riverhog-recovery-tool",
  "module": "a_riverhog_recovery_tool",
  "name": "RecoverySummary",
  "unit": "export"
}
```

</details>
