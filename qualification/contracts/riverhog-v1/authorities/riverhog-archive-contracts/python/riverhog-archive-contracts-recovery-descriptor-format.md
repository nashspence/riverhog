# riverhog_archive_contracts.RECOVERY_DESCRIPTOR_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-recovery-descr-e35ebf0f84:ea95d694bb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2223968e64"></a>
- <a id="s-e34715422f"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-f3d4959d13"></a>`module`: `riverhog_archive_contracts`
- <a id="s-df90d26ecf"></a>`name`: `RECOVERY_DESCRIPTOR_FORMAT`
- <a id="s-fe3e66cb0a"></a>`unit`: `export`

### Declared structure

- <a id="s-3c58f7a9b2"></a>`kind`: `"constant"`
- <a id="s-12948983b1"></a>`value`: `"riverhog-recovery-descriptor/v1"`

## Governing policies

- <a id="pa-a07e8be58f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.RECOVERY_DESCRIPTOR_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 55781cafaccc893f11b235c13448f66440dc17461d28de28c6566515e8158118 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-recovery-descriptor/v1"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "RECOVERY_DESCRIPTOR_FORMAT",
  "unit": "export"
}
```

</details>
