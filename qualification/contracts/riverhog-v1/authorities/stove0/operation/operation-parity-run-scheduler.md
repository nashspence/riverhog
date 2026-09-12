# Operation parity: run_scheduler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: operation:stove0:operation-parity-run-scheduler:11193d6199 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [operation](index.md) |
| Family | [admin](families/admin/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-07bda6fcb7f5"></a>
| Concern | Contract |
|---|---|
| <a id="s-d8e28207e40c"></a>`application` | stove0 |
| <a id="s-299677e1084d"></a>`classification` | human-cli+json |
| <a id="s-49e93cbc7e1b"></a>`cli_commands` | ["scheduler run"] |
| <a id="s-bf5291400987"></a>`client` | Stove0ApiClient |
| <a id="s-82d25cecb136"></a>`method` | POST |
| <a id="s-ec6385721b12"></a>`operation_id` | run_scheduler |
| <a id="s-ecd29cb4a1a6"></a>`path` | /v1/admin/scheduler/run |
| <a id="s-1957b8b3ca48"></a>`provider_evidence` | None |
| <a id="s-336562c9178f"></a>`read_collection` | None |
| <a id="s-6b1e70576cee"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/admin/scheduler/run](../http/post-v1-admin-scheduler-run.md)
- [stove0 scheduler run](../cli/stove0-scheduler-run.md)

## Governing policies

- <a id="pa-5fcc288b858f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de7f)
- <a id="pa-c7dd21bfc9f3"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-3d2492007eb3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b3f) — `scripts/operation_qualification.py::operation_matrix`

### Machine authority

- `/external_contract/operations/117`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37067a8669a5f1333adfc5b51cb4f919826b1256e84020265443093a6ca72bd7 -->

```json
{
  "application": "stove0",
  "classification": "human-cli+json",
  "cli_commands": [
    "scheduler run"
  ],
  "client": "Stove0ApiClient",
  "method": "POST",
  "operation_id": "run_scheduler",
  "path": "/v1/admin/scheduler/run",
  "provider_evidence": null,
  "read_collection": null,
  "response_authority": "operator-projection"
}
```
