# riverhog_ftp_adapter.SourceConfig.complete_policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter:riverhog-ftp-adapter-sourceconfig-complete-policy:903f03e246 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-228339cb3b"></a>
| Field | Shape |
|---|---|
| <a id="s-af46df79d7"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d109ccb240"></a>`distribution` | "riverhog-ftp-adapter" |
| <a id="s-3aa50f4f1a"></a>`module` | "riverhog_ftp_adapter" |
| <a id="s-4817b38c7b"></a>`name` | "complete_policy" |
| <a id="s-4ddc73dbba"></a>`owner` | "riverhog_ftp_adapter.SourceConfig" |
| <a id="s-ac8119d86d"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_ftp_adapter.SourceConfig](riverhog-ftp-adapter-sourceconfig.md)

## Governing policies

- <a id="pa-5a00a681ad"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../../../evidence/sources.md#src-8d11f8fa97) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter.SourceConfig.complete_policy`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ed96419c369a3e24c535fd48008f1c8807310fab280784018ecd9df9c1f58a68 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-ftp-adapter",
  "module": "riverhog_ftp_adapter",
  "name": "complete_policy",
  "owner": "riverhog_ftp_adapter.SourceConfig",
  "unit": "member"
}
```
