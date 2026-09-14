# stove0_core.Stove0RiverhogClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient:bbe326cd05 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7e52965530"></a>
- <a id="s-9822b4afa2"></a>`distribution`: `stove0-server`
- <a id="s-f84704fcb6"></a>`module`: `stove0_core`
- <a id="s-e4c87d889d"></a>`name`: `Stove0RiverhogClient`
- <a id="s-df38f04afd"></a>`unit`: `export`

### Declared structure

- <a id="s-1c0c05238e"></a>`kind`: `"class"`
- <a id="s-4c7369adc7"></a>`signature`: `"\"(api: 'RiverhogApi', *, claim_lease_seconds: 'int' = 1800, capability_ttl_seconds: 'int' = 900, workspace_assurance: 'WorkspaceAssurance' = 'encrypted', claim_purpose: 'str' = 'stove0-collection-work/v1', state: 'WorkStore \| None' = None, authority_batch_size: 'int' = 100) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [abandon_claim](stove0-core-stove0riverhogclient-abandon-claim.md)
- [abandon_preview_claim](stove0-core-stove0riverhogclient-abandon-preview-claim.md)
- [acquire_preview_claim](stove0-core-stove0riverhogclient-acquire-preview-claim.md)
- [acquire_claim](stove0-core-stove0riverhogclient-acquire-claim.md)
- [begin_retirement](stove0-core-stove0riverhogclient-begin-retirement.md)
- [observation_authority](stove0-core-stove0riverhogclient-observation-authority.md)
- [project_target_source_edges](stove0-core-stove0riverhogclient-project-target-source-edges.md)
- [project_target_dispositions](stove0-core-stove0riverhogclient-project-target-dispositions.md)
- [release_claim](stove0-core-stove0riverhogclient-release-claim.md)
- [renew_claim](stove0-core-stove0riverhogclient-renew-claim.md)
- [restart_claim](stove0-core-stove0riverhogclient-restart-claim.md)
- [retire_input](stove0-core-stove0riverhogclient-retire-input.md)
- [seal_execution](stove0-core-stove0riverhogclient-seal-execution.md)
- [seal_target_projection](stove0-core-stove0riverhogclient-seal-target-projection.md)
- [settle_outcomes](stove0-core-stove0riverhogclient-settle-outcomes.md)
- [target_authority](stove0-core-stove0riverhogclient-target-authority.md)
- [verify_and_settle](stove0-core-stove0riverhogclient-verify-and-settle.md)

## Governing policies

- <a id="pa-aa0a73b903"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 870d8d2dd62f6712a3157dec805d6227cc064e2fb923044f5db9f62fa3fcca2f -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(api: 'RiverhogApi', *, claim_lease_seconds: 'int' = 1800, capability_ttl_seconds: 'int' = 900, workspace_assurance: 'WorkspaceAssurance' = 'encrypted', claim_purpose: 'str' = 'stove0-collection-work/v1', state: 'WorkStore | None' = None, authority_batch_size: 'int' = 100) -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "Stove0RiverhogClient",
  "unit": "export"
}
```
