# stove0_protocol.WORKFLOW_PLAN_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflow-plan-format:1351bfc7e2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ec490ca30f"></a>
- <a id="s-6cf1ad5bc3"></a>`distribution`: `stove0-protocol`
- <a id="s-ccac456152"></a>`module`: `stove0_protocol`
- <a id="s-84c874cd67"></a>`name`: `WORKFLOW_PLAN_FORMAT`
- <a id="s-c572ae805f"></a>`unit`: `export`

### Declared structure

- <a id="s-250dab3778"></a>`kind`: `"constant"`
- <a id="s-856205f99b"></a>`value`: `"stove0-workflow-plan/v1"`

## Governing policies

- <a id="pa-ed66a07aa6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WORKFLOW_PLAN_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebb56641d35236524a24c5a69669dd057e8d824e75036feb37934858ac93d343 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-workflow-plan/v1"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WORKFLOW_PLAN_FORMAT",
  "unit": "export"
}
```

</details>
