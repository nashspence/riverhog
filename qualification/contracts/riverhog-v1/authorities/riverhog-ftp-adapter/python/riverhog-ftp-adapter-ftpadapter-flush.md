# riverhog_ftp_adapter.FtpAdapter.flush

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-ftpadapter-flush:aae5a8e10c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-414cd28c54"></a>
| Field | Shape |
|---|---|
| <a id="s-76d5612824"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-904a249fc9"></a>`distribution` | "riverhog-ftp-adapter" |
| <a id="s-b289c64c82"></a>`module` | "riverhog_ftp_adapter" |
| <a id="s-e5deb90e60"></a>`name` | "flush" |
| <a id="s-630c90cb80"></a>`owner` | "riverhog_ftp_adapter.FtpAdapter" |
| <a id="s-53e219b182"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_ftp_adapter.FtpAdapter](riverhog-ftp-adapter-ftpadapter.md)

## Governing policies

- <a id="pa-ddc52acfe9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.FtpAdapter.flush`

### Exact owned JSON

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
