# riverhog_application_access.ARCHIVES_MANAGE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-archives-manage:9a1d6e455c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1c2b5805bb"></a>
- <a id="s-3cc2a03e39"></a>`distribution`: `riverhog-application-access`
- <a id="s-7899364b1a"></a>`module`: `riverhog_application_access`
- <a id="s-c279cc7045"></a>`name`: `ARCHIVES_MANAGE`
- <a id="s-ad0dda27d3"></a>`unit`: `export`

### Declared structure

- <a id="s-525cc1a307"></a>`kind`: `"constant"`
- <a id="s-d6e7fa405d"></a>`value`: `"archives:manage"`

## Governing policies

- <a id="pa-dced800c40"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources/authorities.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.ARCHIVES_MANAGE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f7dd7ffb65483fa9cd47d11c8b2708153f26c5464cac78859c3cd0e570f9a71a -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "archives:manage"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "ARCHIVES_MANAGE",
  "unit": "export"
}
```

</details>
