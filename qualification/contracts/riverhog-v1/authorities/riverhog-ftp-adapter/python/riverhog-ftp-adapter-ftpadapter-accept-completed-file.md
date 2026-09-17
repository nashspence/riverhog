# riverhog_ftp_adapter.FtpAdapter.accept_completed_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-ftpadapter-accept-co-c2fa3618af:b2a92c9578 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ee2bc0293f"></a>
- <a id="s-996a615e3f"></a>`distribution`: `riverhog-ftp-adapter`
- <a id="s-08d482b701"></a>`module`: `riverhog_ftp_adapter`
- <a id="s-b28aaed3f6"></a>`name`: `accept_completed_file`
- <a id="s-be46b15db0"></a>`owner`: `riverhog_ftp_adapter.FtpAdapter`
- <a id="s-4b57426af8"></a>`unit`: `member`

### Declared structure

- <a id="s-ed932784ed"></a>`kind`: `"method"`
- <a id="s-df727f6721"></a>`signature`: `"\"(self, source: 'SourceConfig', path: 'Path', *, relative_path: 'str', source_event_id: 'str', expected_bytes: 'int', expected_sha256: 'str', provenance: 'Mapping[str, object] \| None' = None, provenance_journals: 'Mapping[str, bytes] \| None' = None) -> 'ProducedCollection'\""`

## Maintained corroboration

### Related interface records

- [FtpAdapter](riverhog-ftp-adapter-ftpadapter.md)

## Governing policies

- <a id="pa-0f4549cf40"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — [reference/riverhog/ingress/ftp/src/riverhog\_ftp\_adapter/\_\_init\_\_.py](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.FtpAdapter.accept_completed_file`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d7942f8fae4b087d92dd3c79f05a175505babdc65fff41f5110216161a24fa1e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source: 'SourceConfig', path: 'Path', *, relative_path: 'str', source_event_id: 'str', expected_bytes: 'int', expected_sha256: 'str', provenance: 'Mapping[str, object] | None' = None, provenance_journals: 'Mapping[str, bytes] | None' = None) -> 'ProducedCollection'\""
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "accept_completed_file",
  "owner": "riverhog_ftp_adapter.FtpAdapter",
  "unit": "member"
}
```

</details>
