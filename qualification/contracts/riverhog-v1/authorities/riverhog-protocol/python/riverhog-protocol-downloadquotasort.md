# riverhog_protocol.DownloadQuotaSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-downloadquotasort:9c1f35d9f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-faac0bb56e"></a>
- <a id="s-a3d3352ba2"></a>`distribution`: `riverhog-protocol`
- <a id="s-d623cf015d"></a>`module`: `riverhog_protocol`
- <a id="s-d2941aa99a"></a>`name`: `DownloadQuotaSort`
- <a id="s-568dcb00fa"></a>`unit`: `export`

### Declared structure

- <a id="s-1289b98060"></a>`kind`: `"type-alias"`
- <a id="s-0761086bbd"></a>`value`: `"typing.Literal['app', 'key_id', 'monthly_bytes', 'accounted_bytes', 'reserved_bytes', 'remaining_bytes']"`

## Governing policies

- <a id="pa-9712a65e3e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.DownloadQuotaSort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0a12ba3c04693cdbf08a70735a3619653604fc8719b588c3f6b1affabc5d49d -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['app', 'key_id', 'monthly_bytes', 'accounted_bytes', 'reserved_bytes', 'remaining_bytes']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "DownloadQuotaSort",
  "unit": "export"
}
```

</details>
