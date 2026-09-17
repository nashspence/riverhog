# riverhog_protocol.InvalidState

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-invalidstate:aaaf880e15 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3ee5f40fae"></a>
- <a id="s-a905071161"></a>`distribution`: `riverhog-protocol`
- <a id="s-16287d2f58"></a>`module`: `riverhog_protocol`
- <a id="s-792ea5beb9"></a>`name`: `InvalidState`
- <a id="s-7f02c3ced0"></a>`unit`: `export`

### Declared structure

- <a id="s-24bb53ea59"></a>`kind`: `"class"`
- <a id="s-675d0265df"></a>`signature`: `"\"(message: 'str', *, code: 'str \| None' = None, observed_status: 'int \| None' = None, details: 'dict[str, Any] \| None' = None) -> 'None'\""`

## Governing policies

- <a id="pa-470442ae81"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.InvalidState`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ca2b9148e3ad9cec917102586062169e7272036383b255709a3a29be6f216b4 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(message: 'str', *, code: 'str | None' = None, observed_status: 'int | None' = None, details: 'dict[str, Any] | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "InvalidState",
  "unit": "export"
}
```

</details>
