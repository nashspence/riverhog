# riverhog_protocol.ApplicationKeySort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-applicationkeysort:cc831f02d7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f79baf0d8a"></a>
- <a id="s-3da8b4393f"></a>`distribution`: `riverhog-protocol`
- <a id="s-3776ad2376"></a>`module`: `riverhog_protocol`
- <a id="s-96c0327042"></a>`name`: `ApplicationKeySort`
- <a id="s-d82259694a"></a>`unit`: `export`

### Declared structure

- <a id="s-1d678ca50a"></a>`kind`: `"type-alias"`
- <a id="s-1fd0f4e0ab"></a>`value`: `"typing.Literal['id', 'created_at', 'expires_at', 'last_used_at']"`

## Governing policies

- <a id="pa-b1382a6be6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ApplicationKeySort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c597d3f7c04e9518b2d2b8fa5993b214ba6857d32fd1bb18c1e106ebe5b8a0fc -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['id', 'created_at', 'expires_at', 'last_used_at']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ApplicationKeySort",
  "unit": "export"
}
```

</details>
