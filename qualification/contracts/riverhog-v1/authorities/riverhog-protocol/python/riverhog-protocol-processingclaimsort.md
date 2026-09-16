# riverhog_protocol.ProcessingClaimSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimsort:6f17363552 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ab9a37fed7"></a>
- <a id="s-231fea5c55"></a>`distribution`: `riverhog-protocol`
- <a id="s-b11b76000c"></a>`module`: `riverhog_protocol`
- <a id="s-f5ffdb450a"></a>`name`: `ProcessingClaimSort`
- <a id="s-5c8d3bc499"></a>`unit`: `export`

### Declared structure

- <a id="s-49f1cf423d"></a>`kind`: `"type-alias"`
- <a id="s-6debfe687d"></a>`value`: `"typing.Literal['created_at', 'updated_at', 'expires_at', 'state', 'work_id', 'execution_id']"`

## Governing policies

- <a id="pa-b85dc55e84"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimSort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a1283294bb1024c74579b1133bde0a07d359d1ae32c72895e0aeb638b5f8658 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['created_at', 'updated_at', 'expires_at', 'state', 'work_id', 'execution_id']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimSort",
  "unit": "export"
}
```

</details>
