# riverhog_protocol.DeclaredWorkspaceProtection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-declaredworkspaceprotection:5e328f07c1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8d53cd6b75"></a>
- <a id="s-7d4a9e56c9"></a>`distribution`: `riverhog-protocol`
- <a id="s-b5de0f1cab"></a>`module`: `riverhog_protocol`
- <a id="s-4441a468ab"></a>`name`: `DeclaredWorkspaceProtection`
- <a id="s-26ee75900f"></a>`unit`: `export`

### Declared structure

- <a id="s-b26b98a1a4"></a>`kind`: `"type-alias"`
- <a id="s-236fcc4e6c"></a>`value`: `"typing.Annotated[typing.Literal['encrypted-at-rest', 'memory-backed'], FieldInfo(annotation=NoneType, required=True, description='Deployment declaration for plaintext workspace storage. Memory-backed storage requires no unencrypted swap. The runtime does not verify the mount or swap policy.')]"`

## Governing policies

- <a id="pa-960bb181dd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.DeclaredWorkspaceProtection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a286dae4cf4df871418fb8524121cc27726ee04bd71020ed5980ed8abd4e5c91 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[typing.Literal['encrypted-at-rest', 'memory-backed'], FieldInfo(annotation=NoneType, required=True, description='Deployment declaration for plaintext workspace storage. Memory-backed storage requires no unencrypted swap. The runtime does not verify the mount or swap policy.')]"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "DeclaredWorkspaceProtection",
  "unit": "export"
}
```

</details>
