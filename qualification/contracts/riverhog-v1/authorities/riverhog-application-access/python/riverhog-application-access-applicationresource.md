# riverhog_application_access.ApplicationResource

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-applicationresource:b86729c012 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4ddb6e93d6"></a>
| Field | Shape |
|---|---|
| <a id="s-b44bbb3e30"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-846dc70363"></a>`distribution` | "riverhog-application-access" |
| <a id="s-f17adc8666"></a>`module` | "riverhog_application_access" |
| <a id="s-d4aa6f7501"></a>`name` | "ApplicationResource" |
| <a id="s-8f5d794979"></a>`unit` | "export" |

## Governing policies

- <a id="pa-1944811b79"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.ApplicationResource`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0a391e8908cb6c3fa144ebef1d259ea4c4afbd6493d96fd9c72e4270483b9191 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^(?:\\\\*|tag:.+|collection:[1-9][0-9]*)$', ascii_only=None), AfterValidator(func=<function validate_application_resource>)]"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "ApplicationResource",
  "unit": "export"
}
```
