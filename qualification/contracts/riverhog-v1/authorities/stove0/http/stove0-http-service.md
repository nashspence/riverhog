# stove0 HTTP service

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:stove0-http-service:ac939f84a3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [service](families/service/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Subject | Shape |
|---|---|
| <a id="s-3c2909ec70d2"></a>`info` | additional keys=`version` |
| <a id="s-0a3fa476e610"></a>`openapi` | "3.1.0" |

## Governing policies

- <a id="pa-eac28cf48192"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e3212451) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/info`
- `/external_contract/http_openapi/stove0/openapi`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/http_openapi/stove0/info`

<!-- exact-contract-value: 8efe0b064f3632809c49e59335183be057833f7d28881e38d811f60d7977f104 -->

```json
{
  "title": "stove0",
  "version": "1"
}
```

### `/external_contract/http_openapi/stove0/openapi`

<!-- exact-contract-value: 536c8d78e8a0acbef96c0881c0b313c1dc7090176417df5982b2c7c82423ca16 -->

```json
"3.1.0"
```
