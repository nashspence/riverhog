# riverhog_protocol.RiverhogEventPage.require_progress_after

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-riverhogeventpage-requi-6f307e2646:74f46f15cc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f317919fe5"></a>
- <a id="s-7485fd6061"></a>`distribution`: `riverhog-protocol`
- <a id="s-c1fecc3328"></a>`module`: `riverhog_protocol`
- <a id="s-ce43619ade"></a>`name`: `require_progress_after`
- <a id="s-ace06d6b3d"></a>`owner`: `riverhog_protocol.RiverhogEventPage`
- <a id="s-05c8645cec"></a>`unit`: `member`

### Declared structure

- <a id="s-b2c839a1f5"></a>`kind`: `"method"`
- <a id="s-39b70aabe3"></a>`signature`: `"\"(self, cursor: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [RiverhogEventPage](riverhog-protocol-riverhogeventpage.md)

## Governing policies

- <a id="pa-22e48623f5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.RiverhogEventPage.require_progress_after`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7dc3f81f5f5b43fc63f5f0498ce8ee6d12972b95e100f89e80e63c10b4e4403c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, cursor: 'str') -> 'None'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "require_progress_after",
  "owner": "riverhog_protocol.RiverhogEventPage",
  "unit": "member"
}
```

</details>
