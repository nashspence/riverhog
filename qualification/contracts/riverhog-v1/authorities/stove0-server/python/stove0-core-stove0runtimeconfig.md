# stove0_core.Stove0RuntimeConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0runtimeconfig:4828666681 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0b07556930"></a>
- <a id="s-dae93ab445"></a>`distribution`: `stove0-server`
- <a id="s-358c9bd4a0"></a>`module`: `stove0_core`
- <a id="s-772368c8bb"></a>`name`: `Stove0RuntimeConfig`
- <a id="s-77867083e5"></a>`unit`: `export`

### Declared structure

- <a id="s-27b721241f"></a>`kind`: `"class"`
- <a id="s-7065095398"></a>`signature`: `"\"(database_url: 'str', api_token: 'str \| None', riverhog_base_url: 'str', riverhog_token: 'str', riverhog_allow_insecure_http: 'bool', recipes_path: 'Path', observers: 'dict[str, EndpointRegistration]', targets: 'dict[str, EndpointRegistration]', target_callback_base_url: 'str', target_callback_allow_insecure_http: 'bool', target_callback_signing_key: 'str', target_authority_batch_size: 'int', declared_workspace_protection: 'DeclaredWorkspaceProtection', claim_lease_seconds: 'int', capability_ttl_seconds: 'int', scheduler_interval_seconds: 'float', operational_state_retention_seconds: 'int', browse_token_signing_key: 'str', admissions: 'AdmissionCatalog' = AdmissionCatalog(format='stove0-admissions/v1', policies=()), browse_token_lifetime_seconds: 'int' = 86400) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-8f70e0e8f6"></a>`database_url` | `'str'` | `required` |
| <a id="s-837325d921"></a>`api_token` | `'str \| None'` | `required` |
| <a id="s-767512f5c0"></a>`riverhog_base_url` | `'str'` | `required` |
| <a id="s-a6def3844c"></a>`riverhog_token` | `'str'` | `required` |
| <a id="s-c7810b455c"></a>`riverhog_allow_insecure_http` | `'bool'` | `required` |
| <a id="s-34ff4ac11b"></a>`recipes_path` | `'Path'` | `required` |
| <a id="s-8d22ebcb0b"></a>`observers` | `'dict[str, EndpointRegistration]'` | `required` |
| <a id="s-43dfcd4128"></a>`targets` | `'dict[str, EndpointRegistration]'` | `required` |
| <a id="s-bfa40386df"></a>`target_callback_base_url` | `'str'` | `required` |
| <a id="s-9cbe66812a"></a>`target_callback_allow_insecure_http` | `'bool'` | `required` |
| <a id="s-8000246d58"></a>`target_callback_signing_key` | `'str'` | `required` |
| <a id="s-12cf879f0d"></a>`target_authority_batch_size` | `'int'` | `required` |
| <a id="s-da2857413f"></a>`declared_workspace_protection` | `'DeclaredWorkspaceProtection'` | `required` |
| <a id="s-c6d71379fa"></a>`claim_lease_seconds` | `'int'` | `required` |
| <a id="s-a8de851b2c"></a>`capability_ttl_seconds` | `'int'` | `required` |
| <a id="s-8e3863b3e5"></a>`scheduler_interval_seconds` | `'float'` | `required` |
| <a id="s-a06e8b15d4"></a>`operational_state_retention_seconds` | `'int'` | `required` |
| <a id="s-afef706c39"></a>`browse_token_signing_key` | `'str'` | `required` |
| <a id="s-279cf0be2a"></a>`admissions` | `'AdmissionCatalog'` | `AdmissionCatalog(format='stove0-admissions/v1', policies=())` |
| <a id="s-da08bd651f"></a>`browse_token_lifetime_seconds` | `'int'` | `86400` |

## Maintained corroboration

### Related interface records

- [from_environment](stove0-core-stove0runtimeconfig-from-environment.md)

## Governing policies

- <a id="pa-d649d3d79d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0RuntimeConfig`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 49b87e4e64199838611ccf25e6a940a0394b1e87e7b11593039c9a69db194d11 -->

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
        "name": "declared_workspace_protection",
        "type": "'DeclaredWorkspaceProtection'"
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
    "signature": "\"(database_url: 'str', api_token: 'str | None', riverhog_base_url: 'str', riverhog_token: 'str', riverhog_allow_insecure_http: 'bool', recipes_path: 'Path', observers: 'dict[str, EndpointRegistration]', targets: 'dict[str, EndpointRegistration]', target_callback_base_url: 'str', target_callback_allow_insecure_http: 'bool', target_callback_signing_key: 'str', target_authority_batch_size: 'int', declared_workspace_protection: 'DeclaredWorkspaceProtection', claim_lease_seconds: 'int', capability_ttl_seconds: 'int', scheduler_interval_seconds: 'float', operational_state_retention_seconds: 'int', browse_token_signing_key: 'str', admissions: 'AdmissionCatalog' = AdmissionCatalog(format='stove0-admissions/v1', policies=()), browse_token_lifetime_seconds: 'int' = 86400) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "Stove0RuntimeConfig",
  "unit": "export"
}
```

</details>
