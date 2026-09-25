# a-review0-materializer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-review0-materializer:a-review0-materializer:8ebe951a1b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-34d8f148b6"></a>Parser name: `a-review0-materializer`
- <a id="s-f8605f7d6c"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-d8ac079fe8"></a>`host`<br>`--host` | optional option; 1 value | not recorded | `"127.0.0.1"` |
| <a id="s-d36a4890fe"></a>`port`<br>`--port` | optional option; 1 value | int | `8080` |
| <a id="s-e57c879cfe"></a>`config`<br>`--config` | optional option; 1 value | Path | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-042a375950"></a>`help` | <a id="s-8ae396d032"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-a5c8ef0be3"></a>`0` | <a id="s-ab9ad52942"></a>`"noncontractual-framework-help"` | <a id="s-e224e5e54d"></a>`"empty"` |
| <a id="s-26d8caf5f4"></a>`version` | <a id="s-9fb300c54b"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-061c5bc71e"></a>`0` | <a id="s-b02620391c"></a>`{"distribution":"a-review0-materializer","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-89afbe1cf3"></a>`"empty"` |

### Result and failure contract

- <a id="s-23e8805d19"></a>Result identity: `a-review0-materializer-cli-result/root/v1`
- <a id="s-43e32aa164"></a>Profile: `a-review0-materializer-cli-runtime/v1`
- <a id="s-181b3650ff"></a>Structured output: `none`
- <a id="s-a2d9fa2c00"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-64e45a3a73"></a>`stopped` | <a id="s-f04ce108df"></a>`{"kind":"service-runtime-returned"}` | <a id="s-ff6c712296"></a>`0` | <a id="s-b33506b72c"></a>all: `"no-command-result"` | <a id="s-a37af85f75"></a>all: `"noncontractual-runtime-log"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-00eb48dc8c"></a>`usage` | <a id="s-4ebc160a85"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-9ce670c86d"></a>`2` | <a id="s-5beaa4fa51"></a>all: `"empty"` | <a id="s-183e59c43a"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --host](#s-d8ac079fe8) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --port](#s-d36a4890fe) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --config](#s-e57c879cfe) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-479a9a8f2a"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-1d50036b74"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-review0-materializer](../../../evidence/sources/authorities.md#src-50b76fd01e) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/app.py::&lt;module&gt;](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-review0-materializer/allow_abbrev`
- `/external_contract/cli/a-review0-materializer/name`
- `/external_contract/cli/a-review0-materializer/parameters`
- `/external_contract/cli/a-review0-materializer/result_contract`
- `/external_contract/cli/a-review0-materializer/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-review0-materializer/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-review0-materializer/name`

<!-- exact-contract-value: ecde408d7600af3df4e0622e0f41b1f56761a5bef22f677bf4177c27ad749edc -->

```json
"a-review0-materializer"
```

### `/external_contract/cli/a-review0-materializer/parameters`

<!-- exact-contract-value: f857bd4206ea0bdac2bf007bf3b3c1249a47159b066675265d30304fb215c8b3 -->

```json
[
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
    "default": 8080,
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
    "dest": "config",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--config"
    ],
    "required": false,
    "type": "Path"
  }
]
```

### `/external_contract/cli/a-review0-materializer/result_contract`

<!-- exact-contract-value: d79109a4a094d8322a885e37fdbf00153db20ccee665f23372512041d8b8bc46 -->

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
  "identity": "a-review0-materializer-cli-result/root/v1",
  "profile_id": "a-review0-materializer-cli-runtime/v1",
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

### `/external_contract/cli/a-review0-materializer/terminating_controls`

<!-- exact-contract-value: 2fc27a04036a760b34329a2ddf5e18483767d2ac2ae0a01baa4c4d7ea3b09aeb -->

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
  },
  {
    "exit_status": 0,
    "id": "version",
    "stderr": "empty",
    "stdout": {
      "distribution": "a-review0-materializer",
      "kind": "installed-coordinated-release-version",
      "serialization": "noncontractual"
    },
    "trigger": {
      "kind": "option-present",
      "options": [
        "--version"
      ]
    }
  }
]
```

</details>
