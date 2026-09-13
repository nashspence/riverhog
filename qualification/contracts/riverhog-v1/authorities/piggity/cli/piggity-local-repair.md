# piggity local repair

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-repair:0b1434d45e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-9e8311971e"></a>Parser name: `repair`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-3f96fb6ed3"></a>`wait` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --wait |
| <a id="s-0caf5df218"></a>`restore_policy` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --restore-policy |
| <a id="s-6cd35e75ee"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-6cd35e75ee) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --restore-policy](#s-0caf5df218) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --wait](#s-3f96fb6ed3) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/catalog/collections/{collection_id}/inventory](../../riverhog/http-operations/get-v1-catalog-collections-collection-id-inventory.md)
- [GET /v1/collections/{collection_id}/tags](../../riverhog/http-operations/get-v1-collections-collection-id-tags.md)
- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [GET /v1/retrieval-jobs/{job_id}/content](../../riverhog/http-operations/get-v1-retrieval-jobs-job-id-content.md)
- [GET /v1/retrieval-jobs/{job_id}](../../riverhog/http-operations/get-v1-retrieval-jobs-job-id.md)
- [GET /v1/retrieval-plans/{plan_id}/files](../../riverhog/http-operations/get-v1-retrieval-plans-plan-id-files.md)
- [POST /v1/retrieval-jobs/{job_id}/ack](../../riverhog/http-operations/post-v1-retrieval-jobs-job-id-ack.md)
- [POST /v1/retrieval-jobs/{job_id}/renew](../../riverhog/http-operations/post-v1-retrieval-jobs-job-id-renew.md)
- [POST /v1/retrieval-jobs](../../riverhog/http-operations/post-v1-retrieval-jobs.md)
- [POST /v1/retrieval-plans/{plan_id}/advance](../../riverhog/http-operations/post-v1-retrieval-plans-plan-id-advance.md)
- [POST /v1/retrieval-plans](../../riverhog/http-operations/post-v1-retrieval-plans.md)

## Governing policies

- <a id="pa-9d2b136bb4"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-4bef3e76d9"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/repair/name`
- `/external_contract/cli/piggity/commands/local/commands/repair/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/repair/name`

<!-- exact-contract-value: 15b490ee176bfe3f1065942fd75feec8e02c2d9beeaca063b55a973f0da8c0e4 -->

```json
"repair"
```

### `/external_contract/cli/piggity/commands/local/commands/repair/parameters`

<!-- exact-contract-value: 6d6f63e6cd222ae7a2b7a7bcbac898d6478aced320f5378d56f9d313ff59ebd7 -->

```json
[
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "wait",
    "nargs": 1,
    "options": [
      "--wait"
    ],
    "required": false,
    "secondary_options": [
      "--no-wait"
    ],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  },
  {
    "count": false,
    "default": "allow",
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "restore_policy",
    "nargs": 1,
    "options": [
      "--restore-policy"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "json_mode",
    "nargs": 1,
    "options": [
      "--json"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  }
]
```
