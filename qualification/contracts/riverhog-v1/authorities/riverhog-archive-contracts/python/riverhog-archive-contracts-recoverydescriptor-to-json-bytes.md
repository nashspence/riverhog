# riverhog_archive_contracts.RecoveryDescriptor.to_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-recoverydescri-a0c0551eb3:8812992842 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4932e90ecb"></a>
- <a id="s-4eb08b6b71"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-ebd2c41ef4"></a>`module`: `riverhog_archive_contracts`
- <a id="s-88804cd6d5"></a>`name`: `to_json_bytes`
- <a id="s-425fe2d6ce"></a>`owner`: `riverhog_archive_contracts.RecoveryDescriptor`
- <a id="s-ddff61dd70"></a>`unit`: `member`

### Declared structure

- <a id="s-2ecc819ae5"></a>`kind`: `"method"`
- <a id="s-290a49ac56"></a>`signature`: `"\"(self) -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [RecoveryDescriptor](riverhog-archive-contracts-recoverydescriptor.md)

## Governing policies

- <a id="pa-cbef4115b0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.RecoveryDescriptor.to_json_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13b076d6f0381478dcec034920764934f97d6f6c3600446145b60a905b3e5c2a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "to_json_bytes",
  "owner": "riverhog_archive_contracts.RecoveryDescriptor",
  "unit": "member"
}
```

</details>
