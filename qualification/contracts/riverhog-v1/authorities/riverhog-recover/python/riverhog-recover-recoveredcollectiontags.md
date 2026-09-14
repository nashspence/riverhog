# riverhog_recover.RecoveredCollectionTags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-recover:riverhog-recover-recoveredcollectiontags:cf33023cdf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-recover](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-27f06669e4"></a>
- <a id="s-55cc6af245"></a>`distribution`: `riverhog-recover`
- <a id="s-c6b2b1e297"></a>`module`: `riverhog_recover`
- <a id="s-d1a05c530e"></a>`name`: `RecoveredCollectionTags`
- <a id="s-203b117ad8"></a>`unit`: `export`

### Declared structure

- <a id="s-2e1ff84b72"></a>`kind`: `"class"`
- <a id="s-20b431a885"></a>`signature`: `"\"(*, archive: 'Path', passphrase: 'str', age_command: 'str', head: 'CollectionTagHeadDocument') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [riverhog_recover.RecoveredCollectionTags.iter_tags](riverhog-recover-recoveredcollectiontags-iter-tags.md)

## Governing policies

- <a id="pa-705dcef9f7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-recover:riverhog_recover](../../../evidence/sources.md#src-dbfe6c5e2e) — `reference/riverhog/recovery/src/riverhog_recover/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_recover.RecoveredCollectionTags`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6b604ab061c0b8785a519f46f5eab5725ad0d48efe1ce24075604f9093d65273 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, archive: 'Path', passphrase: 'str', age_command: 'str', head: 'CollectionTagHeadDocument') -> 'None'\""
  },
  "distribution": "riverhog-recover",
  "module": "riverhog_recover",
  "name": "RecoveredCollectionTags",
  "unit": "export"
}
```
