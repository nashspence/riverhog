# riverhog_archive_contracts.CollectionTreeIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectiontreeidentity:52528f3fc5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-43ee0a3714"></a>
- <a id="s-6a9e01fbb2"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-fc0ea8c532"></a>`module`: `riverhog_archive_contracts`
- <a id="s-043d240528"></a>`name`: `CollectionTreeIdentity`
- <a id="s-6128210eb1"></a>`unit`: `export`

### Declared structure

- <a id="s-df41020481"></a>`kind`: `"class"`
- <a id="s-273fe6efb2"></a>`signature`: `"\"(files: 'int', bytes: 'int', sha256: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-155ac80c23"></a>`files` | `'int'` | `required` |
| <a id="s-fa299d954c"></a>`bytes` | `'int'` | `required` |
| <a id="s-e6085e4191"></a>`sha256` | `'str'` | `required` |

## Maintained corroboration

### Related interface records

- [from_mapping](riverhog-archive-contracts-collectiontreeidentity-from-mapping.md)
- [to_mapping](riverhog-archive-contracts-collectiontreeidentity-to-mapping.md)

## Governing policies

- <a id="pa-e200d31f2a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionTreeIdentity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4002c9165fb469c2b924607f0a0e45512a705a50e9fb6313a25e4580c03fcae2 -->

```json
{
  "contract": {
    "fields": [
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
        "name": "sha256",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(files: 'int', bytes: 'int', sha256: 'str') -> None\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "CollectionTreeIdentity",
  "unit": "export"
}
```

</details>
