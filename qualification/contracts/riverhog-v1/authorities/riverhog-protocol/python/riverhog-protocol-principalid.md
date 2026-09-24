# riverhog_protocol.PrincipalId

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-principalid:2a88c8a2be -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cace598cd3"></a>
- <a id="s-bc689f1dea"></a>`distribution`: `riverhog-protocol`
- <a id="s-3c9c8b5888"></a>`module`: `riverhog_protocol`
- <a id="s-a8b505b426"></a>`name`: `PrincipalId`
- <a id="s-c8cc55870b"></a>`unit`: `export`

### Declared structure

- <a id="s-30d2735ed6"></a>`kind`: `"type-alias"`
- <a id="s-1389a1735d"></a>`value`: `"typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^(?:[a-z0-9]+(?:-[a-z0-9]+)*\|claim:[0-9a-f]{64}\|processing:[0-9a-f]{64})$')]), AfterValidator(func=<function validate_principal_id>)]"`

## Governing policies

- <a id="pa-324ef37af5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.PrincipalId`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8004712800efaa6320bbabb50a18fe07a4c76176fbf5094a8a78f7cf1fa96218 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^(?:[a-z0-9]+(?:-[a-z0-9]+)*|claim:[0-9a-f]{64}|processing:[0-9a-f]{64})$')]), AfterValidator(func=<function validate_principal_id>)]"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "PrincipalId",
  "unit": "export"
}
```

</details>
