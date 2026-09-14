# riverhog_archive_contracts.RECOVERY_DESCRIPTOR_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-recovery-descriptor-path:dc266dc0cc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bb4a2f292a"></a>
- <a id="s-a5a49e2c4e"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-7dde2d8300"></a>`module`: `riverhog_archive_contracts`
- <a id="s-16a199e55a"></a>`name`: `RECOVERY_DESCRIPTOR_PATH`
- <a id="s-3ddc827695"></a>`unit`: `export`

### Declared structure

- <a id="s-1adb7b5554"></a>`kind`: `"constant"`
- <a id="s-a9780a8694"></a>`value`: `"recovery.json"`

## Governing policies

- <a id="pa-c855d89d50"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.RECOVERY_DESCRIPTOR_PATH`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40bfed5278f653e698b9fd77b26561bca0c70fbaca63c7047300808f8d15b18a -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "recovery.json"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "RECOVERY_DESCRIPTOR_PATH",
  "unit": "export"
}
```
