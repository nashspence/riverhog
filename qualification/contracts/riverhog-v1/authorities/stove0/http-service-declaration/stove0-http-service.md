# stove0 HTTP service

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-service-declaration:stove0:stove0-http-service:32f5afb790 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Service Declaration](index.md) |

## External contract

<a id="s-3c2909ec70"></a>

| Field | Value |
|---|---|
| <a id="s-572009ad8a"></a>`info · title` | `"stove0"` |
| <a id="s-c761e20736"></a>`info · version` | `"1"` |
| <a id="s-0a3fa476e6"></a>`openapi` | `"3.1.0"` |

## Governing policies

- <a id="pa-362b5aae7d"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/info`
- `/external_contract/http_openapi/stove0/openapi`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
