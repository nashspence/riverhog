# riverhog_archive_contracts.PACK_INDEX_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-pack-index-format:4a3cd8f604 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0dba66d9a5"></a>
- <a id="s-cd9286ca00"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-c4542a3d21"></a>`module`: `riverhog_archive_contracts`
- <a id="s-f12f8e7abd"></a>`name`: `PACK_INDEX_FORMAT`
- <a id="s-25f59d74a3"></a>`unit`: `export`

### Declared structure

- <a id="s-e5be6bd3ef"></a>`kind`: `"constant"`
- <a id="s-fd29d7dada"></a>`value`: `"riverhog-pack-index/v1"`

## Governing policies

- <a id="pa-45b9c2cc20"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.PACK_INDEX_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6c063f6f6e916b6f3df36794ba502f548b103cba926c30557ed69f72e46870fd -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-pack-index/v1"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "PACK_INDEX_FORMAT",
  "unit": "export"
}
```

</details>
