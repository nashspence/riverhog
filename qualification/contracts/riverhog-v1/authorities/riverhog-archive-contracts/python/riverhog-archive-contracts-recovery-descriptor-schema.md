# riverhog_archive_contracts.RECOVERY_DESCRIPTOR_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-recovery-descr-c91baf4ac0:4745a6e60f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e680da1301"></a>
- <a id="s-826427c433"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-40706fdc2b"></a>`module`: `riverhog_archive_contracts`
- <a id="s-4c951e9066"></a>`name`: `RECOVERY_DESCRIPTOR_SCHEMA`
- <a id="s-278756da83"></a>`unit`: `export`

### Declared structure

- <a id="s-23b0aab4a3"></a>`kind`: `"constant"`
- <a id="s-f2c3c14773"></a>`value`: `"riverhog-recovery-descriptor/v1"`

## Governing policies

- <a id="pa-f4fa156786"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.RECOVERY_DESCRIPTOR_SCHEMA`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c2795dff8a6a0478ee1deff86adfc26c6e60161a40f6c87830b3dc25d85dc44 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-recovery-descriptor/v1"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "RECOVERY_DESCRIPTOR_SCHEMA",
  "unit": "export"
}
```

</details>
