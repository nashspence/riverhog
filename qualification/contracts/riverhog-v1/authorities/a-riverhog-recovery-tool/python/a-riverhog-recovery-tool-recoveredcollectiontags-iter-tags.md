# a_riverhog_recovery_tool.RecoveredCollectionTags.iter_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-recovery-tool:a-riverhog-recovery-tool-recoveredcollect-110bed96b0:ca75c7ba11 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-recovery-tool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-111096dd90"></a>
- <a id="s-78aedf000e"></a>`distribution`: `a-riverhog-recovery-tool`
- <a id="s-bb6dfad8ed"></a>`module`: `a_riverhog_recovery_tool`
- <a id="s-31c399fa08"></a>`name`: `iter_tags`
- <a id="s-15c0481c2e"></a>`owner`: `a_riverhog_recovery_tool.RecoveredCollectionTags`
- <a id="s-34445aaa6d"></a>`unit`: `member`

### Declared structure

- <a id="s-1f3264ba26"></a>`kind`: `"method"`
- <a id="s-df0bac08c2"></a>`signature`: `"\"(self) -> 'Iterator[str]'\""`

## Maintained corroboration

### Related interface records

- [RecoveredCollectionTags](a-riverhog-recovery-tool-recoveredcollectiontags.md)

## Governing policies

- <a id="pa-059d8b4544"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-recovery-tool:a_riverhog_recovery_tool](../../../evidence/sources/authorities.md#src-a085f6d973) — [some-implementations/riverhog/recovery/src/a\_riverhog\_recovery\_tool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_recovery_tool.RecoveredCollectionTags.iter_tags`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e5aba12d5d2598b352685be7fc8a0816d660fa25bbd66141d817f05148e08175 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Iterator[str]'\""
  },
  "distribution": "a-riverhog-recovery-tool",
  "module": "a_riverhog_recovery_tool",
  "name": "iter_tags",
  "owner": "a_riverhog_recovery_tool.RecoveredCollectionTags",
  "unit": "member"
}
```

</details>
