# riverhog_application_access.permission_covers

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-permission-covers:3873c8b417 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e827416471"></a>
- <a id="s-4e031de658"></a>`distribution`: `riverhog-application-access`
- <a id="s-640c6041f2"></a>`module`: `riverhog_application_access`
- <a id="s-c10eab7ac0"></a>`name`: `permission_covers`
- <a id="s-720fa90b05"></a>`unit`: `export`

### Declared structure

- <a id="s-bc9e57fe80"></a>`kind`: `"function"`
- <a id="s-a4e6a230ce"></a>`signature`: `"\"(grantor: 'str', requested: 'str') -> 'bool'\""`

## Governing policies

- <a id="pa-8ea5ac72b5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources/authorities.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.permission_covers`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 932414779653b9105df2a8ee5e385839a6b7cd9ab470eb6cf8487e07b411fabc -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(grantor: 'str', requested: 'str') -> 'bool'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "permission_covers",
  "unit": "export"
}
```

</details>
