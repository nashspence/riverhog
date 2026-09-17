# riverhog_application_access.KEYS_MANAGE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-keys-manage:ee747d3538 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f97cdeaeb"></a>
- <a id="s-9ee34f12ad"></a>`distribution`: `riverhog-application-access`
- <a id="s-292c05b153"></a>`module`: `riverhog_application_access`
- <a id="s-1824ae3286"></a>`name`: `KEYS_MANAGE`
- <a id="s-8991a9a854"></a>`unit`: `export`

### Declared structure

- <a id="s-0f9920ca03"></a>`kind`: `"constant"`
- <a id="s-ee56beaa37"></a>`value`: `"keys:manage"`

## Governing policies

- <a id="pa-1d17216321"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources/authorities.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.KEYS_MANAGE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7c02c5f804e10130beb341ea819f4ab14bef1d892593a3b0b98f28d06d1bbf6d -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "keys:manage"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "KEYS_MANAGE",
  "unit": "export"
}
```

</details>
