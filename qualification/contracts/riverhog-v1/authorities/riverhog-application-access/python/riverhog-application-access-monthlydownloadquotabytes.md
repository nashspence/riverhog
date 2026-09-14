# riverhog_application_access.MonthlyDownloadQuotaBytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-monthlydownlo-cbd505bca9:2812dcc6f9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ac5a86c8f2"></a>
- <a id="s-39a6072289"></a>`distribution`: `riverhog-application-access`
- <a id="s-9d442c1464"></a>`module`: `riverhog_application_access`
- <a id="s-460ba1882f"></a>`name`: `MonthlyDownloadQuotaBytes`
- <a id="s-21855468ae"></a>`unit`: `export`

### Declared structure

- <a id="s-dc5b24469b"></a>`kind`: `"type-alias"`
- <a id="s-368d4cd3fd"></a>`value`: `"typing.Annotated[int, BeforeValidator(func=<function validate_monthly_download_quota_bytes>, json_schema_input_type=PydanticUndefined), FieldInfo(annotation=NoneType, required=True, metadata=[Ge(ge=0)])]"`

## Governing policies

- <a id="pa-7540d89716"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.MonthlyDownloadQuotaBytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fed71c145b0d0749c85d0ac9b947a71f4687964692ab86f20f9f9725867a18e7 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[int, BeforeValidator(func=<function validate_monthly_download_quota_bytes>, json_schema_input_type=PydanticUndefined), FieldInfo(annotation=NoneType, required=True, metadata=[Ge(ge=0)])]"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "MonthlyDownloadQuotaBytes",
  "unit": "export"
}
```
