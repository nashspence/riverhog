# riverhog_archive_contracts.StoredPartIdentity.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-storedpartiden-9cdff6592c:570204e671 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dd0ecadd0c"></a>
- <a id="s-7ed9b99f3b"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-df731783f2"></a>`module`: `riverhog_archive_contracts`
- <a id="s-bb290b8487"></a>`name`: `from_mapping`
- <a id="s-e4ef36c725"></a>`owner`: `riverhog_archive_contracts.StoredPartIdentity`
- <a id="s-ddd5407a91"></a>`unit`: `member`

### Declared structure

- <a id="s-1c8830e216"></a>`kind`: `"classmethod"`
- <a id="s-9f983272da"></a>`signature`: `"\"(cls, value: 'object', *, expected_number: 'int', expected_start: 'int') -> 'StoredPartIdentity'\""`

## Maintained corroboration

### Related interface records

- [StoredPartIdentity](riverhog-archive-contracts-storedpartidentity.md)

## Governing policies

- <a id="pa-9a9a688042"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.StoredPartIdentity.from_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 731228b9f5470c908ab856568b64f6c04bdd85177697250557c77912e31b1175 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'object', *, expected_number: 'int', expected_start: 'int') -> 'StoredPartIdentity'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "from_mapping",
  "owner": "riverhog_archive_contracts.StoredPartIdentity",
  "unit": "member"
}
```

</details>
