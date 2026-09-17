# riverhog_application_access.ALL_PERMISSIONS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-all-permissions:af007485a9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c85027805f"></a>
- <a id="s-c262f451f9"></a>`distribution`: `riverhog-application-access`
- <a id="s-99f313db51"></a>`module`: `riverhog_application_access`
- <a id="s-07c2dae067"></a>`name`: `ALL_PERMISSIONS`
- <a id="s-fad03e9270"></a>`unit`: `export`

### Declared structure

- <a id="s-5eb0e6cb45"></a>`kind`: `"constant"`
- <a id="s-fde36df979"></a>`value`: `"*"`

## Governing policies

- <a id="pa-a4925f82c2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources/authorities.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.ALL_PERMISSIONS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4012bc8650faa886697ef1728f03fb124d695b9bf02f1b615c5bb6094ee31d8d -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "*"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "ALL_PERMISSIONS",
  "unit": "export"
}
```

</details>
