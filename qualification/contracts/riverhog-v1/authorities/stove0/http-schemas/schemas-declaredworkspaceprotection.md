# schemas: DeclaredWorkspaceProtection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-declaredworkspaceprotection:283401be24 -->

Deployment declaration for plaintext workspace storage. Memory-backed storage requires no unencrypted swap. The runtime does not verify the mount or swap policy.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-a2987c540b"></a>

- <a id="s-91b22ff714"></a>`type`: `"string"`
- <a id="s-13dafff73a"></a>`enum`: `["encrypted-at-rest","memory-backed"]`
- <a id="s-5ace8da5f7"></a>`description`: `"Deployment declaration for plaintext workspace storage. Memory-backed storage requires no unencrypted swap. The runtime does not verify the mount or swap policy."`

## Governing policies

- <a id="pa-2a754fa1ca"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/DeclaredWorkspaceProtection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 64ac3a7149081a0a4c1453019dfcbb2dd9e1456a4aebfc3cba3a2c1fc2d9f616 -->

```json
{
  "description": "Deployment declaration for plaintext workspace storage. Memory-backed storage requires no unencrypted swap. The runtime does not verify the mount or swap policy.",
  "enum": [
    "encrypted-at-rest",
    "memory-backed"
  ],
  "type": "string"
}
```

</details>
