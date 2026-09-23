# a-riverhog-ftp-spool listen

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-ftp-spool:a-riverhog-ftp-spool-listen:5fb34acbe0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-7671d34507"></a>Parser name: `listen`
- <a id="s-7ba9c9a286"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-979af6c742"></a>`source`<br>`--source` | required option; 1 value | not recorded | not recorded |
| <a id="s-3a798d9b2f"></a>`username`<br>`--username` | required option; 1 value | not recorded | not recorded |
| <a id="s-4ef8186722"></a>`password_file`<br>`--password-file` | required option; 1 value | Path | not recorded |
| <a id="s-6c1272826e"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-b7953f7d5f"></a>`port`<br>`--port` | optional option; 1 value | int | `2121` |
| <a id="s-3c098d1e30"></a>`passive_port_start`<br>`--passive-port-start` | optional option; 1 value | int | `30000` |
| <a id="s-ffe7c7227c"></a>`passive_port_end`<br>`--passive-port-end` | optional option; 1 value | int | `30039` |
| <a id="s-b0a5c93757"></a>`public_host`<br>`--public-host` | optional option; 1 value | not recorded | not recorded |
| <a id="s-32981a79a5"></a>`max_connections`<br>`--max-connections` | optional option; 1 value | int | `256` |
| <a id="s-2efa580755"></a>`max_connections_per_ip`<br>`--max-connections-per-ip` | optional option; 1 value | int | `32` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-9dad94b599"></a>`help` | <a id="s-e720093232"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-aa30a032c4"></a>`0` | <a id="s-528f988424"></a>`"noncontractual-framework-help"` | <a id="s-0294e06f8c"></a>`"empty"` |

### Result and failure contract

- <a id="s-e2481a9195"></a>Result identity: `a-riverhog-ftp-spool-cli-result/listen/v1`
- <a id="s-53bced9bf3"></a>Profile: `a-riverhog-ftp-spool-cli-runtime/v1`
- <a id="s-f3e2e67e12"></a>Structured output: `none`
- <a id="s-57224a7fb2"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0731c91d1b"></a>`stopped` | <a id="s-63cfa0ae1c"></a>`{"kind":"service-runtime-returned"}` | <a id="s-b5fe69ec1e"></a>`0` | <a id="s-5cf722c32d"></a>all: `"no-command-result"` | <a id="s-6a84879e4b"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1b0a437ebf"></a>`usage` | <a id="s-226e83efd4"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-bd7ba82d4d"></a>`2` | <a id="s-4a514ecbbf"></a>all: `"empty"` | <a id="s-5439370e69"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --source](#s-979af6c742) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --username](#s-3a798d9b2f) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --password-file](#s-4ef8186722) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --host](#s-6c1272826e) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-b7953f7d5f) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --passive-port-start](#s-3c098d1e30) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --passive-port-end](#s-ffe7c7227c) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --public-host](#s-b0a5c93757) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --max-connections](#s-32981a79a5) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --max-connections-per-ip](#s-2efa580755) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-967aa4bb16"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-2ea1d0d5dd"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-4f9bc5584a) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-ftp-spool/commands/listen/allow_abbrev`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/listen/name`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/listen/parameters`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/listen/result_contract`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/listen/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-ftp-spool/commands/listen/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/listen/name`

<!-- exact-contract-value: 3237ced043ee02810536760dbe5970a8ad7ba6591bc86fe3f7beb4530ad746c6 -->

```json
"listen"
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/listen/parameters`

<!-- exact-contract-value: 16bec56e9da277c3cfdde2175af9cafb1d17d696bd2668c018e041094e381aac -->

```json
[
  {
    "dest": "source",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--source"
    ],
    "required": true
  },
  {
    "dest": "username",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--username"
    ],
    "required": true
  },
  {
    "dest": "password_file",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--password-file"
    ],
    "required": true,
    "type": "Path"
  },
  {
    "default": "127.0.0.1",
    "dest": "host",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--host"
    ],
    "required": false
  },
  {
    "default": 2121,
    "dest": "port",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--port"
    ],
    "required": false,
    "type": "int"
  },
  {
    "default": 30000,
    "dest": "passive_port_start",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--passive-port-start"
    ],
    "required": false,
    "type": "int"
  },
  {
    "default": 30039,
    "dest": "passive_port_end",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--passive-port-end"
    ],
    "required": false,
    "type": "int"
  },
  {
    "dest": "public_host",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--public-host"
    ],
    "required": false
  },
  {
    "default": 256,
    "dest": "max_connections",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--max-connections"
    ],
    "required": false,
    "type": "int"
  },
  {
    "default": 32,
    "dest": "max_connections_per_ip",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--max-connections-per-ip"
    ],
    "required": false,
    "type": "int"
  }
]
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/listen/result_contract`

<!-- exact-contract-value: a3f7583452cb29566a668500b7c571580f7d5261167e6f9548421e2dbd11e979 -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "selected_by": {
        "kind": "parser-rejected-invocation"
      },
      "stderr": {
        "all": "noncontractual-usage-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "not-applicable",
  "identity": "a-riverhog-ftp-spool-cli-result/listen/v1",
  "profile_id": "a-riverhog-ftp-spool-cli-runtime/v1",
  "structured_output": "none",
  "success": [
    {
      "exit_status": 0,
      "id": "stopped",
      "selected_by": {
        "kind": "service-runtime-returned"
      },
      "stderr": {
        "all": "noncontractual-runtime-log"
      },
      "stdout": {
        "all": "no-command-result"
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/listen/terminating_controls`

<!-- exact-contract-value: 46c96c22d2ed8a51da57bba3ac0f2269bb35f98c5fd6dc3e3ba67e60f772ad72 -->

```json
[
  {
    "exit_status": 0,
    "id": "help",
    "stderr": "empty",
    "stdout": "noncontractual-framework-help",
    "trigger": {
      "kind": "option-present",
      "options": [
        "-h",
        "--help"
      ]
    }
  }
]
```

</details>
