# riverhog_application_access.ApplicationKeyId

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-applicationkeyid:a82835e5d7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-84218914d5"></a>
- <a id="s-a742629677"></a>`distribution`: `riverhog-application-access`
- <a id="s-e6a3265e0e"></a>`module`: `riverhog_application_access`
- <a id="s-0763fec9be"></a>`name`: `ApplicationKeyId`
- <a id="s-19d547567b"></a>`unit`: `export`

### Declared structure

- <a id="s-97f515e0b1"></a>`kind`: `"type-alias"`
- <a id="s-04d5a22d55"></a>`value`: `"typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{16}$')]), AfterValidator(func=<function validate_application_key_id>)]"`

## Governing policies

- <a id="pa-d2ab5801ca"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.ApplicationKeyId`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b79ec16e07e8f5cd65ff65baf50f1bc3a304347ef5798da443d644d62378811d -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[0-9a-f]{16}$')]), AfterValidator(func=<function validate_application_key_id>)]"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "ApplicationKeyId",
  "unit": "export"
}
```

</details>
