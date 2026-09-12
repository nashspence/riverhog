# stove0 HTTP service

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:stove0-http-service:ac939f84a3 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `service` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/stove0/info`
- `/external_contract/http_openapi/stove0/openapi`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Contract summary

- Shape: array (2 items)

## Complete owned contract

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
