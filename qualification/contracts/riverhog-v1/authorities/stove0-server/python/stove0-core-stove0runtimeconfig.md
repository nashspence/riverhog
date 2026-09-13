# stove0_core.Stove0RuntimeConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0runtimeconfig:4828666681 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0b07556930"></a>
| Field | Shape |
|---|---|
| <a id="s-d97746b4d4"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-dae93ab445"></a>`distribution` | "stove0-server" |
| <a id="s-358c9bd4a0"></a>`module` | "stove0_core" |
| <a id="s-772368c8bb"></a>`name` | "Stove0RuntimeConfig" |
| <a id="s-77867083e5"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0RuntimeConfig.from_environment](stove0-core-stove0runtimeconfig-from-environment.md)

## Governing policies

- <a id="pa-d649d3d79d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RuntimeConfig`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e286a4bd2966f13bf4b76bdcf851cd3b8d4029024dd90c6472d9ea25154aaf44 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "database_url",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "api_token",
        "type": "'str | None'"
      },
      {
        "default": "required",
        "name": "riverhog_base_url",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "riverhog_token",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "riverhog_allow_insecure_http",
        "type": "'bool'"
      },
      {
        "default": "required",
        "name": "recipes_path",
        "type": "'Path'"
      },
      {
        "default": "required",
        "name": "observers",
        "type": "'dict[str, EndpointRegistration]'"
      },
      {
        "default": "required",
        "name": "targets",
        "type": "'dict[str, EndpointRegistration]'"
      },
      {
        "default": "required",
        "name": "target_callback_base_url",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "target_callback_allow_insecure_http",
        "type": "'bool'"
      },
      {
        "default": "required",
        "name": "target_callback_signing_key",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "target_authority_batch_size",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "workspace_assurance",
        "type": "\"Literal['encrypted', 'ephemeral']\""
      },
      {
        "default": "required",
        "name": "claim_lease_seconds",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "capability_ttl_seconds",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "scheduler_interval_seconds",
        "type": "'float'"
      },
      {
        "default": "required",
        "name": "operational_state_retention_seconds",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "browse_token_signing_key",
        "type": "'str'"
      },
      {
        "default": "AdmissionCatalog(format='stove0-admissions/v1', policies=())",
        "name": "admissions",
        "type": "'AdmissionCatalog'"
      },
      {
        "default": "86400",
        "name": "browse_token_lifetime_seconds",
        "type": "'int'"
      }
    ],
    "kind": "class",
    "signature": "'(database_url: \\'str\\', api_token: \\'str | None\\', riverhog_base_url: \\'str\\', riverhog_token: \\'str\\', riverhog_allow_insecure_http: \\'bool\\', recipes_path: \\'Path\\', observers: \\'dict[str, EndpointRegistration]\\', targets: \\'dict[str, EndpointRegistration]\\', target_callback_base_url: \\'str\\', target_callback_allow_insecure_http: \\'bool\\', target_callback_signing_key: \\'str\\', target_authority_batch_size: \\'int\\', workspace_assurance: \"Literal[\\'encrypted\\', \\'ephemeral\\']\", claim_lease_seconds: \\'int\\', capability_ttl_seconds: \\'int\\', scheduler_interval_seconds: \\'float\\', operational_state_retention_seconds: \\'int\\', browse_token_signing_key: \\'str\\', admissions: \\'AdmissionCatalog\\' = AdmissionCatalog(format=\\'stove0-admissions/v1\\', policies=()), browse_token_lifetime_seconds: \\'int\\' = 86400) -> None'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "Stove0RuntimeConfig",
  "unit": "export"
}
```
