# riverhog_application_access.validate_application_key_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-validate-appl-696abb1efe:e7347e2ed7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-237fc55740"></a>
- <a id="s-c2dfab5941"></a>`distribution`: `riverhog-application-access`
- <a id="s-305e2d011e"></a>`module`: `riverhog_application_access`
- <a id="s-e54f4d4878"></a>`name`: `validate_application_key_id`
- <a id="s-36a46f358c"></a>`unit`: `export`

### Declared structure

- <a id="s-cf6b748b90"></a>`kind`: `"function"`
- <a id="s-7a2cce2d09"></a>`signature`: `"\"(value: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-838363aa12"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.validate_application_key_id`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dae28621c3a91f8bed9391ef110a6c79f6a3080c1f9efb75857843d36e92039f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'str') -> 'str'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "validate_application_key_id",
  "unit": "export"
}
```

</details>
