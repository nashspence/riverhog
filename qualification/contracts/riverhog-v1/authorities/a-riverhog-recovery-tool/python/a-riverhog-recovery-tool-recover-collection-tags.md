# a_riverhog_recovery_tool.recover_collection_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-recovery-tool:a-riverhog-recovery-tool-recover-collection-tags:c2b0cdf1a6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-recovery-tool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-53178685a2"></a>
- <a id="s-c99b191e57"></a>`distribution`: `a-riverhog-recovery-tool`
- <a id="s-9dcea52a63"></a>`module`: `a_riverhog_recovery_tool`
- <a id="s-6f152fb692"></a>`name`: `recover_collection_tags`
- <a id="s-fb49853856"></a>`unit`: `export`

### Declared structure

- <a id="s-1ac80ed824"></a>`kind`: `"function"`
- <a id="s-979a0fbe11"></a>`signature`: `"\"(archive_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'RecoveredCollectionTags'\""`

## Governing policies

- <a id="pa-556dbbdd0c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-recovery-tool:a_riverhog_recovery_tool](../../../evidence/sources/authorities.md#src-a085f6d973) — [some-implementations/riverhog/recovery/src/a\_riverhog\_recovery\_tool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_recovery_tool.recover_collection_tags`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5bb32df93d5f86e8f18c5b3011cb00dc628b923fc303a310316d6e66c51b2137 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(archive_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'RecoveredCollectionTags'\""
  },
  "distribution": "a-riverhog-recovery-tool",
  "module": "a_riverhog_recovery_tool",
  "name": "recover_collection_tags",
  "unit": "export"
}
```

</details>
