# riverhog_provenance.PROVENANCE_VOLUME_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-volume-format:37a6b1e1dc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d0e64d47e5"></a>
- <a id="s-1f7cabd687"></a>`distribution`: `riverhog-provenance`
- <a id="s-4c9a0c22fc"></a>`module`: `riverhog_provenance`
- <a id="s-c6038a2fb0"></a>`name`: `PROVENANCE_VOLUME_FORMAT`
- <a id="s-167b5ad9aa"></a>`unit`: `export`

### Declared structure

- <a id="s-c09991e041"></a>`kind`: `"constant"`
- <a id="s-5b088503d9"></a>`value`: `"riverhog-provenance-volume/v1"`

## Governing policies

- <a id="pa-9c3e9133d6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_VOLUME_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a5c753b47b5714e53077790c4619fd5508711c32ad3631787baae7b199a320c8 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-volume/v1"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_VOLUME_FORMAT",
  "unit": "export"
}
```

</details>
