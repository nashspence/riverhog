# riverhog_client.ApplicationResource

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-applicationresource:2e3b3f1078 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e82152dea7"></a>
| Field | Shape |
|---|---|
| <a id="s-e7bd15ef2d"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-e8b9618125"></a>`distribution` | "riverhog-client" |
| <a id="s-6d7fb7843a"></a>`module` | "riverhog_client" |
| <a id="s-e239e7c8bc"></a>`name` | "ApplicationResource" |
| <a id="s-1f2eb4ce60"></a>`unit` | "export" |

## Governing policies

- <a id="pa-7463779f44"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApplicationResource`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e70a1522f86d3ec120585985025cef6fc078f27b890e60bd0f52466670546cc2 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^(?:\\\\*|tag:.+|collection:[1-9][0-9]*)$', ascii_only=None), AfterValidator(func=<function validate_application_resource>)]"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "ApplicationResource",
  "unit": "export"
}
```
