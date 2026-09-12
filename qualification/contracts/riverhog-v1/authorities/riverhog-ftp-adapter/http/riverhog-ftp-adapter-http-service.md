# riverhog-ftp-adapter HTTP service

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog-ftp-adapter:riverhog-ftp-adapter-http-service:71fb456dfd -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter` |
| Interface | `http` |
| Family | `service` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/info`
- `/external_contract/http_openapi/riverhog-ftp-adapter/openapi`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog-ftp-adapter` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract

```json
[
  {
    "description": "Content-opaque FTP collection producer.",
    "title": "Riverhog FTP adapter API",
    "version": "1.0.0"
  },
  "3.1.0"
]
```
