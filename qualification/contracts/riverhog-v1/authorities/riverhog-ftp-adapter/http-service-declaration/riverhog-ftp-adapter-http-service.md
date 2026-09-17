# riverhog-ftp-adapter HTTP service

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-service-declaration:riverhog-ftp-adapter:riverhog-ftp-adapter-http-service:d0204c759f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [HTTP Service Declaration](index.md) |

## External contract

<a id="s-ca90b0c459"></a>

| Field | Value |
|---|---|
| <a id="s-147a848294"></a>`info · description` | `"Content-opaque FTP collection producer."` |
| <a id="s-1833c49b61"></a>`info · title` | `"Riverhog FTP adapter API"` |
| <a id="s-7c6aa9ce29"></a>`info · version` | `"1.0.0"` |
| <a id="s-e4c15d9272"></a>`openapi` | `"3.1.0"` |

## Governing policies

- <a id="pa-50e8fd242e"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog-ftp-adapter](../../../evidence/sources/authorities.md#src-c3a51ac29a) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog-ftp-adapter/info`
- `/external_contract/http_openapi/riverhog-ftp-adapter/openapi`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
