# riverhog HTTP service

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-service-declaration:riverhog:riverhog-http-service:139f6c3b1d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Service Declaration](index.md) |

## External contract

<a id="s-5a7411c0c7"></a>

| Field | Value |
|---|---|
| <a id="s-75a2539115"></a>`info · title` | `"riverhog API"` |
| <a id="s-c078620f1f"></a>`info · version` | `"0.1.0"` |
| <a id="s-2e1f916040"></a>`openapi` | `"3.1.0"` |

## Governing policies

- <a id="pa-ca2bc8b190"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/info`
- `/external_contract/http_openapi/riverhog/openapi`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
