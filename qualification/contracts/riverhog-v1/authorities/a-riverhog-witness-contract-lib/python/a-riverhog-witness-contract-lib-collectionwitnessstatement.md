# a_riverhog_witness_contract_lib.CollectionWitnessStatement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-witness-contract-lib:a-riverhog-witness-contract-lib-collectio-bd8216e5ae:cd5ca49427 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-witness-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b89d60329c"></a>
- <a id="s-90765d3950"></a>`distribution`: `a-riverhog-witness-contract-lib`
- <a id="s-a3c306e7b3"></a>`module`: `a_riverhog_witness_contract_lib`
- <a id="s-d862142d9d"></a>`name`: `CollectionWitnessStatement`
- <a id="s-8e1cd62a2f"></a>`unit`: `export`

### Declared structure

- <a id="s-48aa647ee3"></a>`kind`: `"class"`
- <a id="s-aa7d5f364b"></a>`signature`: `"\"(source_identity: 'str', collection_id: 'int', archive_root_sha256: 'str', content_identity: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-68ea98d9b2"></a>`source_identity` | `'str'` | `required` |
| <a id="s-936528ce10"></a>`collection_id` | `'int'` | `required` |
| <a id="s-250f2b2243"></a>`archive_root_sha256` | `'str'` | `required` |
| <a id="s-9ae8e3b68d"></a>`content_identity` | `'str'` | `required` |

## Maintained corroboration

### Related interface records

- [serialize](a-riverhog-witness-contract-lib-collectionwitnessstatement-serialize.md)
- [from_catalog](a-riverhog-witness-contract-lib-collectionwitnessstatement-from-catalog.md)
- [sha256](a-riverhog-witness-contract-lib-collectionwitnessstatement-sha256.md)
- [parse](a-riverhog-witness-contract-lib-collectionwitnessstatement-parse.md)

## Governing policies

- <a id="pa-4f709633c1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-witness-contract-lib:a_riverhog_witness_contract_lib](../../../evidence/sources/authorities.md#src-3c53e0f06b) — [some-implementations/riverhog/packages/a-riverhog-witness-contract-lib/src/a\_riverhog\_witness\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/packages/a-riverhog-witness-contract-lib/src/a_riverhog_witness_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_witness_contract_lib.CollectionWitnessStatement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a80660128b8790b776366580b1e979e078104cb765849d53c652d53363fabe3 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "source_identity",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "collection_id",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "archive_root_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "content_identity",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(source_identity: 'str', collection_id: 'int', archive_root_sha256: 'str', content_identity: 'str') -> None\""
  },
  "distribution": "a-riverhog-witness-contract-lib",
  "module": "a_riverhog_witness_contract_lib",
  "name": "CollectionWitnessStatement",
  "unit": "export"
}
```

</details>
