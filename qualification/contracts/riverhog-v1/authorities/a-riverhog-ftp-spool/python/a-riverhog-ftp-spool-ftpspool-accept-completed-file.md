# a_riverhog_ftp_spool.FtpSpool.accept_completed_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-ftp-spool:a-riverhog-ftp-spool-ftpspool-accept-completed-file:6b235b1a22 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-996522ad87"></a>
- <a id="s-12c5dba9f0"></a>`distribution`: `a-riverhog-ftp-spool`
- <a id="s-46e361c544"></a>`module`: `a_riverhog_ftp_spool`
- <a id="s-db006956dd"></a>`name`: `accept_completed_file`
- <a id="s-018f26c190"></a>`owner`: `a_riverhog_ftp_spool.FtpSpool`
- <a id="s-a6afa262b0"></a>`unit`: `member`

### Declared structure

- <a id="s-d7863a61e3"></a>`kind`: `"method"`
- <a id="s-7b704eef63"></a>`signature`: `"\"(self, source: 'SourceConfig', path: 'Path', *, relative_path: 'str', source_event_id: 'str', expected_bytes: 'int', expected_sha256: 'str', provenance: 'Mapping[str, object] \| None' = None, provenance_journals: 'Mapping[str, bytes] \| None' = None) -> 'ProducedCollection'\""`

## Maintained corroboration

### Related interface records

- [FtpSpool](a-riverhog-ftp-spool-ftpspool.md)

## Governing policies

- <a id="pa-cb04b0c985"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-ftp-spool:a_riverhog_ftp_spool](../../../evidence/sources/authorities.md#src-ab8cadf46c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_ftp_spool.FtpSpool.accept_completed_file`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7d86b886514896b39d03e1ed96b8015bd4de2ddf806dfb202a53c8dfa92906a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source: 'SourceConfig', path: 'Path', *, relative_path: 'str', source_event_id: 'str', expected_bytes: 'int', expected_sha256: 'str', provenance: 'Mapping[str, object] | None' = None, provenance_journals: 'Mapping[str, bytes] | None' = None) -> 'ProducedCollection'\""
  },
  "distribution": "a-riverhog-ftp-spool",
  "module": "a_riverhog_ftp_spool",
  "name": "accept_completed_file",
  "owner": "a_riverhog_ftp_spool.FtpSpool",
  "unit": "member"
}
```

</details>
