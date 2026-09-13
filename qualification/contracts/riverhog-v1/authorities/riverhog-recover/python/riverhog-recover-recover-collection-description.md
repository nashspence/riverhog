# riverhog_recover.recover_collection_description

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-recover:riverhog-recover-recover-collection-description:a817e70a5e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-recover](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d9f3694a6b"></a>
| Field | Shape |
|---|---|
| <a id="s-1a9731e181"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-8c95a83c36"></a>`distribution` | "riverhog-recover" |
| <a id="s-f4dfeb7db0"></a>`module` | "riverhog_recover" |
| <a id="s-bb2412afa8"></a>`name` | "recover_collection_description" |
| <a id="s-183c9274f3"></a>`unit` | "export" |

## Governing policies

- <a id="pa-af95514be9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-recover:riverhog_recover](../../../evidence/sources.md#src-dbfe6c5e2e) — `reference/riverhog/recovery/src/riverhog_recover/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_recover.recover_collection_description`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 465c3d5e2be05591d7346167224969ee7dd20008e5c88697f25a9b5cfebc0b80 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(archive_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'CollectionDescriptionDocument | None'\""
  },
  "distribution": "riverhog-recover",
  "module": "riverhog_recover",
  "name": "recover_collection_description",
  "unit": "export"
}
```
