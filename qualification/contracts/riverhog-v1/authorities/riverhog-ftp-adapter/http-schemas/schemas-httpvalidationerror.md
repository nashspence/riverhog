# schemas: HTTPValidationError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog-ftp-adapter:schemas-httpvalidationerror:4226ae2952 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-e56876550a"></a>

- <a id="s-3f107bb300"></a>`type`: `"object"`
- <a id="s-80163497bd"></a>`title`: `"HTTPValidationError"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8f6dcc5be5"></a>`detail` | no | type="array"; items=([ValidationError](schemas-validationerror.md)); title="Detail" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog-ftp-adapter"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field detail](#s-8f6dcc5be5) | `cardinality · items · operational_policy` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [ValidationError](schemas-validationerror.md)

## Governing policies

- <a id="pa-087c1f5f43"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-116263e2a3"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog-ftp-adapter](../../../evidence/sources.md#src-c3a51ac29a) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/components/schemas/HTTPValidationError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7373c21f1389312367e27e440140ba11d587693c56d8aa27249b36d3f58c10c6 -->

```json
{
  "properties": {
    "detail": {
      "items": {
        "$ref": "#/components/schemas/ValidationError"
      },
      "title": "Detail",
      "type": "array"
    }
  },
  "title": "HTTPValidationError",
  "type": "object"
}
```

</details>
