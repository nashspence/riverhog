# a_riverhog_recovery_tool.RecoveredCollectionTags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-recovery-tool:a-riverhog-recovery-tool-recoveredcollectiontags:f5f637a6d8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-recovery-tool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3ba984321f"></a>
- <a id="s-12f8dd5c9d"></a>`distribution`: `a-riverhog-recovery-tool`
- <a id="s-1d35cb5951"></a>`module`: `a_riverhog_recovery_tool`
- <a id="s-73db0b89c2"></a>`name`: `RecoveredCollectionTags`
- <a id="s-c051813bac"></a>`unit`: `export`

### Declared structure

- <a id="s-c7fea94d81"></a>`kind`: `"class"`
- <a id="s-302e075595"></a>`signature`: `"\"(*, archive: 'Path', passphrase: 'str', age_command: 'str', head: 'CollectionTagHeadDocument') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [iter_tags](a-riverhog-recovery-tool-recoveredcollectiontags-iter-tags.md)

## Governing policies

- <a id="pa-c111ab9c41"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-recovery-tool:a_riverhog_recovery_tool](../../../evidence/sources/authorities.md#src-a085f6d973) — [some-implementations/riverhog/recovery/src/a\_riverhog\_recovery\_tool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_recovery_tool.RecoveredCollectionTags`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb0d741b97fc874d33f7535fec795e991c591fdfc70e1ca538b6148391510fb2 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, archive: 'Path', passphrase: 'str', age_command: 'str', head: 'CollectionTagHeadDocument') -> 'None'\""
  },
  "distribution": "a-riverhog-recovery-tool",
  "module": "a_riverhog_recovery_tool",
  "name": "RecoveredCollectionTags",
  "unit": "export"
}
```

</details>
