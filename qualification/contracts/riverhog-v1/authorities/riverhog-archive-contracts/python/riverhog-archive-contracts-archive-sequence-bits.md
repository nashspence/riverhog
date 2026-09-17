# riverhog_archive_contracts.ARCHIVE_SEQUENCE_BITS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-archive-sequence-bits:d900441045 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8f4aa675d0"></a>
- <a id="s-c3a32a1662"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-e55bff0ba1"></a>`module`: `riverhog_archive_contracts`
- <a id="s-d2b35a0d1a"></a>`name`: `ARCHIVE_SEQUENCE_BITS`
- <a id="s-3327c2446c"></a>`unit`: `export`

### Declared structure

- <a id="s-a7ba21a457"></a>`kind`: `"constant"`
- <a id="s-5a8aa3283b"></a>`value`: `256`

## Governing policies

- <a id="pa-ef4015b531"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.ARCHIVE_SEQUENCE_BITS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0b3f8470724170ad4d0ef6a8081756cd7b83aa813cadb4ad99fda3a13af6a78a -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 256
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "ARCHIVE_SEQUENCE_BITS",
  "unit": "export"
}
```

</details>
