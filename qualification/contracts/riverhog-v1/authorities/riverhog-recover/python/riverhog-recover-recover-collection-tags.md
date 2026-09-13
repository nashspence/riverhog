# riverhog_recover.recover_collection_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-recover:riverhog-recover-recover-collection-tags:f42c4212df -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-recover](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2e13ff64ac"></a>
| Field | Shape |
|---|---|
| <a id="s-c09679a40c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-4e4f918b6a"></a>`distribution` | "riverhog-recover" |
| <a id="s-27e360283d"></a>`module` | "riverhog_recover" |
| <a id="s-7672a6525a"></a>`name` | "recover_collection_tags" |
| <a id="s-fc0c07af02"></a>`unit` | "export" |

## Governing policies

- <a id="pa-ecdb51e874"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-recover:riverhog_recover](../../../evidence/sources.md#src-dbfe6c5e2e) — `reference/riverhog/recovery/src/riverhog_recover/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_recover.recover_collection_tags`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c874d8288aa9629ee694fdca71adc5ab433d2a3544f2ae9b4a771de936d0ed3f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(archive_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'RecoveredCollectionTags'\""
  },
  "distribution": "riverhog-recover",
  "module": "riverhog_recover",
  "name": "recover_collection_tags",
  "unit": "export"
}
```
