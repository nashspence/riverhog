# riverhog_application_access.CATALOG_READ

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-catalog-read:bc734dc15d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ae8a3836a5"></a>
- <a id="s-0c6bbbf235"></a>`distribution`: `riverhog-application-access`
- <a id="s-56e29ad8dc"></a>`module`: `riverhog_application_access`
- <a id="s-d827bd474d"></a>`name`: `CATALOG_READ`
- <a id="s-3ce44b27b9"></a>`unit`: `export`

### Declared structure

- <a id="s-47a778a40b"></a>`kind`: `"constant"`
- <a id="s-6812221fdf"></a>`value`: `"catalog:read"`

## Governing policies

- <a id="pa-6e631ac6a1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources/authorities.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.CATALOG_READ`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4175ba2d8c6f145e6c3716763516b1875488ade26813b4c062d284c730b20bb4 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "catalog:read"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "CATALOG_READ",
  "unit": "export"
}
```

</details>
