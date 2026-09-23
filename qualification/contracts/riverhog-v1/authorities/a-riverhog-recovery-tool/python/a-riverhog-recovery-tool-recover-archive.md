# a_riverhog_recovery_tool.recover_archive

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-recovery-tool:a-riverhog-recovery-tool-recover-archive:73da8a934f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-recovery-tool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-62ccb9f97b"></a>
- <a id="s-79b364b5b1"></a>`distribution`: `a-riverhog-recovery-tool`
- <a id="s-772e935d8c"></a>`module`: `a_riverhog_recovery_tool`
- <a id="s-6856e69cea"></a>`name`: `recover_archive`
- <a id="s-ef798e2c88"></a>`unit`: `export`

### Declared structure

- <a id="s-26bf0df3e8"></a>`kind`: `"function"`
- <a id="s-81800cf901"></a>`signature`: `"\"(archive_dir: 'Path', output_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'RecoverySummary'\""`

## Governing policies

- <a id="pa-bffb8fa947"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-recovery-tool:a_riverhog_recovery_tool](../../../evidence/sources/authorities.md#src-a085f6d973) — [some-implementations/riverhog/recovery/src/a\_riverhog\_recovery\_tool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_recovery_tool.recover_archive`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be909ebd8b34a8a44ec2d238c11e2a9a83c8efa7d7696a3808596fc4ec231908 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(archive_dir: 'Path', output_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'RecoverySummary'\""
  },
  "distribution": "a-riverhog-recovery-tool",
  "module": "a_riverhog_recovery_tool",
  "name": "recover_archive",
  "unit": "export"
}
```

</details>
