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

<a id="s-07bda6fcb7"></a>
| Concern | Contract |
|---|---|
| <a id="s-d8e28207e4"></a>`application` | stove0 |
| <a id="s-299677e108"></a>`classification` | human-cli+json |
| <a id="s-49e93cbc7e"></a>`cli_commands` | ["scheduler run"] |
| <a id="s-bf52914009"></a>`client` | Stove0ApiClient |
| <a id="s-82d25cecb1"></a>`method` | POST |
| <a id="s-ec6385721b"></a>`operation_id` | run_scheduler |
| <a id="s-ecd29cb4a1"></a>`path` | /v1/admin/scheduler/run |
| <a id="s-1957b8b3ca"></a>`provider_evidence` | None |
| <a id="s-336562c917"></a>`read_collection` | None |
| <a id="s-6b1e70576c"></a>`response_authority` | operator-projection |

## Maintained corroboration

### Related interface records

- [POST /v1/admin/scheduler/run](../http/post-v1-admin-scheduler-run.md)
- [stove0-client scheduler run](../../stove0-client/cli/stove0-client-scheduler-run.md)

## Governing policies

- <a id="pa-5fcc288b85"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c7dd21bfc9"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-3d2492007e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [operations:operation-matrix](../../../evidence/sources.md#src-b032bdc56b) — `scripts/operation_qualification.py::operation_matrix`

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
