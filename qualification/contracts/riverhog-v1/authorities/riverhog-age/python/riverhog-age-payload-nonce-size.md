# riverhog_age.PAYLOAD_NONCE_SIZE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-payload-nonce-size:243748c738 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6eed9d37ef"></a>
- <a id="s-51cb42b543"></a>`distribution`: `riverhog-age`
- <a id="s-af399fbc71"></a>`module`: `riverhog_age`
- <a id="s-829a5468c3"></a>`name`: `PAYLOAD_NONCE_SIZE`
- <a id="s-e54d25928b"></a>`unit`: `export`

### Declared structure

- <a id="s-4a86a99720"></a>`kind`: `"constant"`
- <a id="s-ec421eb4d9"></a>`value`: `16`

## Governing policies

- <a id="pa-77dfa07291"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources/authorities.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.PAYLOAD_NONCE_SIZE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ed65c97e51277498c29e44ef40704ffa7002d5c1d4805caf5989e9192a39db77 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 16
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "PAYLOAD_NONCE_SIZE",
  "unit": "export"
}
```

</details>
