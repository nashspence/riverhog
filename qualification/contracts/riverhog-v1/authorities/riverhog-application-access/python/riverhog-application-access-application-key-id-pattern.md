# riverhog_application_access.APPLICATION_KEY_ID_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-application-k-9e63a07308:fde04910b2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a8eab2a1bd"></a>
- <a id="s-aaadfc5526"></a>`distribution`: `riverhog-application-access`
- <a id="s-c14cd6830c"></a>`module`: `riverhog_application_access`
- <a id="s-aed7b0771e"></a>`name`: `APPLICATION_KEY_ID_PATTERN`
- <a id="s-6468a309c6"></a>`unit`: `export`

### Declared structure

- <a id="s-8fc0b5f33f"></a>`kind`: `"constant"`
- <a id="s-a918867dc7"></a>`value`: `"^[0-9a-f]{16}$"`

## Governing policies

- <a id="pa-29327627dd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources/authorities.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.APPLICATION_KEY_ID_PATTERN`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f580f245625fc3e4476bb092b622e5f6a4d8af4aa521cae64bfc0832fd58b43 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^[0-9a-f]{16}$"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "APPLICATION_KEY_ID_PATTERN",
  "unit": "export"
}
```

</details>
