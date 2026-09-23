# a-riverhog-ftp-spool HTTP service

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-service-declaration:a-riverhog-ftp-spool:a-riverhog-ftp-spool-http-service:a7325d44e8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [HTTP Service Declaration](index.md) |

## External contract

<a id="s-39cadb77a7"></a>

| Field | Value |
|---|---|
| <a id="s-bd1162785a"></a>`info · description` | `"Content-opaque FTP collection producer."` |
| <a id="s-b5adc34747"></a>`info · title` | `"Riverhog FTP spool API"` |
| <a id="s-8cec8231dc"></a>`info · version` | `"1.0.0"` |
| <a id="s-2e3e414e51"></a>`openapi` | `"3.1.0"` |

## Governing policies

- <a id="pa-38f90c352f"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-fdb5f95db7) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/a-riverhog-ftp-spool/info`
- `/external_contract/http_openapi/a-riverhog-ftp-spool/openapi`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/http_openapi/a-riverhog-ftp-spool/info`

<!-- exact-contract-value: 08966ae9305815505ed53d16d08052a396d51358abfa986da6e9d1d52839fbe6 -->

```json
{
  "description": "Content-opaque FTP collection producer.",
  "title": "Riverhog FTP spool API",
  "version": "1.0.0"
}
```

### `/external_contract/http_openapi/a-riverhog-ftp-spool/openapi`

<!-- exact-contract-value: 536c8d78e8a0acbef96c0881c0b313c1dc7090176417df5982b2c7c82423ca16 -->

```json
"3.1.0"
```

</details>
