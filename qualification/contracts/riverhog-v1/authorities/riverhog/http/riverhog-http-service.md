# riverhog HTTP service

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:riverhog-http-service:ab0334a16c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [service](families/service/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Subject | Shape |
|---|---|
| <a id="s-5a7411c0c709"></a>`info` | additional keys=`version` |
| <a id="s-2e1f91604045"></a>`openapi` | "3.1.0" |

## Governing policies

- <a id="pa-36aada742e87"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/info`
- `/external_contract/http_openapi/riverhog/openapi`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/http_openapi/riverhog/info`

<!-- exact-contract-value: 63c58fb42b0faf830caf4de555bc17b6921244a8cc0c58406dbd2fe34afddb39 -->

```json
{
  "title": "riverhog API",
  "version": "0.1.0"
}
```

### `/external_contract/http_openapi/riverhog/openapi`

<!-- exact-contract-value: 536c8d78e8a0acbef96c0881c0b313c1dc7090176417df5982b2c7c82423ca16 -->

```json
"3.1.0"
```
