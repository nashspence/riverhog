# riverhog-ftp-adapter HTTP service

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog-ftp-adapter:riverhog-ftp-adapter-http-service:71fb456dfd -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `http` |
| Family | `service` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- Shape: items=additional keys=`version` | "3.1.0"

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog-ftp-adapter` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

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
