# riverhog_ftp_adapter.FtpAdapter.flush

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-ftpadapter-flush:aae5a8e10c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-414cd28c54"></a>
- <a id="s-904a249fc9"></a>`distribution`: `riverhog-ftp-adapter`
- <a id="s-b289c64c82"></a>`module`: `riverhog_ftp_adapter`
- <a id="s-e5deb90e60"></a>`name`: `flush`
- <a id="s-630c90cb80"></a>`owner`: `riverhog_ftp_adapter.FtpAdapter`
- <a id="s-53e219b182"></a>`unit`: `member`

### Declared structure

- <a id="s-f467de5ec7"></a>`kind`: `"method"`
- <a id="s-c89290d1c9"></a>`signature`: `"\"(self, source_id: 'str') -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [FtpAdapter](riverhog-ftp-adapter-ftpadapter.md)

## Governing policies

- <a id="pa-ddc52acfe9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources/authorities.md#src-8d11f8fa97) — [reference/riverhog/ingress/ftp/src/riverhog\_ftp\_adapter/\_\_init\_\_.py](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.FtpAdapter.flush`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75fb643318b8309b301de53b0efd29fd0e95f5212a452b7c628ffc4739328cf8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source_id: 'str') -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "flush",
  "owner": "riverhog_ftp_adapter.FtpAdapter",
  "unit": "member"
}
```

</details>
