# a_riverhog_recovery_tool.recover_collection_description

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-recovery-tool:a-riverhog-recovery-tool-recover-collecti-b1307ed8b0:f3a3d98312 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-recovery-tool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7e8e3a550d"></a>
- <a id="s-7032540142"></a>`distribution`: `a-riverhog-recovery-tool`
- <a id="s-7b64da3613"></a>`module`: `a_riverhog_recovery_tool`
- <a id="s-f3bddcd232"></a>`name`: `recover_collection_description`
- <a id="s-90cc480279"></a>`unit`: `export`

### Declared structure

- <a id="s-85954403e1"></a>`kind`: `"function"`
- <a id="s-88a91c4808"></a>`signature`: `"\"(archive_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'CollectionDescriptionDocument \| None'\""`

## Governing policies

- <a id="pa-5c16692158"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-recovery-tool:a_riverhog_recovery_tool](../../../evidence/sources/authorities.md#src-a085f6d973) — [some-implementations/riverhog/recovery/src/a\_riverhog\_recovery\_tool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_recovery_tool.recover_collection_description`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af071f76d9ff7768a431c3566982cb9cb31a36e983472da98f22b3a319bd84f2 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(archive_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'CollectionDescriptionDocument | None'\""
  },
  "distribution": "a-riverhog-recovery-tool",
  "module": "a_riverhog_recovery_tool",
  "name": "recover_collection_description",
  "unit": "export"
}
```

</details>
