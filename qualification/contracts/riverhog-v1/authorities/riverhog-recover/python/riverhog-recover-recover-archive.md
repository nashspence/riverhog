# riverhog_recover.recover_archive

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-recover:riverhog-recover-recover-archive:697682d158 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-recover](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-46d6d88e34"></a>
- <a id="s-6892e61931"></a>`distribution`: `riverhog-recover`
- <a id="s-d6cd35f849"></a>`module`: `riverhog_recover`
- <a id="s-b170351150"></a>`name`: `recover_archive`
- <a id="s-7101dec922"></a>`unit`: `export`

### Declared structure

- <a id="s-35e0e533af"></a>`kind`: `"function"`
- <a id="s-ea2029d95e"></a>`signature`: `"\"(archive_dir: 'Path', output_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'RecoverySummary'\""`

## Governing policies

- <a id="pa-80b18bf22e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-recover:riverhog_recover](../../../evidence/sources.md#src-dbfe6c5e2e) — `reference/riverhog/recovery/src/riverhog_recover/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_recover.recover_archive`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 584d99e3aa1feaf24284483e23c46f78152da2c263c0bb5cb9273e73b1469b9b -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(archive_dir: 'Path', output_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'RecoverySummary'\""
  },
  "distribution": "riverhog-recover",
  "module": "riverhog_recover",
  "name": "recover_archive",
  "unit": "export"
}
```
