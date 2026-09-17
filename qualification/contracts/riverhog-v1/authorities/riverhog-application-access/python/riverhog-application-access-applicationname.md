# riverhog_application_access.ApplicationName

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-applicationname:069deebf74 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c7712d0c51"></a>
- <a id="s-0dfdc021ed"></a>`distribution`: `riverhog-application-access`
- <a id="s-3074bf46f5"></a>`module`: `riverhog_application_access`
- <a id="s-465bc6dabd"></a>`name`: `ApplicationName`
- <a id="s-e465efe344"></a>`unit`: `export`

### Declared structure

- <a id="s-c07cbbf291"></a>`kind`: `"type-alias"`
- <a id="s-fc744caddd"></a>`value`: `"typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[a-z0-9]+(?:-[a-z0-9]+)*$')]), AfterValidator(func=<function validate_application_name>)]"`

## Governing policies

- <a id="pa-76af1d3852"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources/authorities.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.ApplicationName`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12ca6844e451a3f4655543957ff1d2f42222754cd5bb25977544290b53b8e908 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[a-z0-9]+(?:-[a-z0-9]+)*$')]), AfterValidator(func=<function validate_application_name>)]"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "ApplicationName",
  "unit": "export"
}
```

</details>
