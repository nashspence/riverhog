# riverhog_ftp_adapter.SourceConfig.absolute_root

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-sourceconfig-absolute-root:c4fd5d5dbb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6efd4a17a9"></a>
- <a id="s-a3719a08d3"></a>`distribution`: `riverhog-ftp-adapter`
- <a id="s-736989e7c9"></a>`module`: `riverhog_ftp_adapter`
- <a id="s-163892fbf1"></a>`name`: `absolute_root`
- <a id="s-ef26842c8c"></a>`owner`: `riverhog_ftp_adapter.SourceConfig`
- <a id="s-8c0930dd8d"></a>`unit`: `member`

### Declared structure

- <a id="s-49355e4b0b"></a>`kind`: `"classmethod"`
- <a id="s-11b3376b7a"></a>`signature`: `"\"(cls, value: 'Path') -> 'Path'\""`

## Maintained corroboration

### Related interface records

- [SourceConfig](riverhog-ftp-adapter-sourceconfig.md)

## Governing policies

- <a id="pa-949ff7e1e1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — [reference/riverhog/ingress/ftp/src/riverhog\_ftp\_adapter/\_\_init\_\_.py](../../../../../../reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.SourceConfig.absolute_root`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 225a97b800eb3f6084ec2470f815a66f4d7e3cca5b218c973b28707cd4e8a2a3 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Path') -> 'Path'\""
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "absolute_root",
  "owner": "riverhog_ftp_adapter.SourceConfig",
  "unit": "member"
}
```

</details>
