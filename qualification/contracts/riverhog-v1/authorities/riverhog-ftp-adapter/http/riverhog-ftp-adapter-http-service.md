# riverhog-ftp-adapter HTTP service

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog-ftp-adapter:riverhog-ftp-adapter-http-service:71fb456dfd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [http](index.md) |
| Family | [service](index.md#f-55a18408bd) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Subject | Shape |
|---|---|
| <a id="s-ca90b0c459"></a>`info` | additional keys=`version` |
| <a id="s-e4c15d9272"></a>`openapi` | "3.1.0" |

## Governing policies

- <a id="pa-6b164a3897"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog-ftp-adapter](../../../evidence/sources.md#src-c3a51ac29a) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/info`
- `/external_contract/http_openapi/riverhog-ftp-adapter/openapi`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/http_openapi/riverhog-ftp-adapter/info`

<!-- exact-contract-value: a3062f0d32090b5cbbce7d4d5b2b9dab62abfd8a44af244a359dbb58496e5593 -->

```json
{
  "description": "Content-opaque FTP collection producer.",
  "title": "Riverhog FTP adapter API",
  "version": "1.0.0"
}
```

### `/external_contract/http_openapi/riverhog-ftp-adapter/openapi`

<!-- exact-contract-value: 536c8d78e8a0acbef96c0881c0b313c1dc7090176417df5982b2c7c82423ca16 -->

```json
"3.1.0"
```
