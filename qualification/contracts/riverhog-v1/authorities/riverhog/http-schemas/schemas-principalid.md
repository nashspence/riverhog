# schemas: PrincipalId

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-principalid:b916781dfc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-474ab2f2e5"></a>

- <a id="s-ccb258e06c"></a>`type`: `"string"`
- <a id="s-a160e8e327"></a>`pattern`: `"^(?:[a-z0-9]+(?:-[a-z0-9]+)*\|claim:[0-9a-f]{64}\|processing:[0-9a-f]{64})$"`

## Governing policies

- <a id="pa-1803179734"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/PrincipalId`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 749d07b5e899f4a12e8f77f17a8dcbf1236e8d4b619e1c32cb3f14408785e382 -->

```json
{
  "pattern": "^(?:[a-z0-9]+(?:-[a-z0-9]+)*|claim:[0-9a-f]{64}|processing:[0-9a-f]{64})$",
  "type": "string"
}
```

</details>
