# stove0-observer-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-observer-support:stove0-observer-conformance:b986442d80 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-72fdebec40"></a>Parser name: `stove0-observer-conformance`
- <a id="s-0b86bb01fd"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-6504128d72"></a>`base_url` | required positional; 1 value | not recorded | not recorded |
| <a id="s-3ce127cbc3"></a>`invocation`<br>`--invocation` | optional option; 1 value; collects repeats; no declared occurrence maximum | Path | `[]` |
| <a id="s-38ac0c67ea"></a>`semantic_vectors`<br>`--semantic-vectors` | optional option; 1 value; collects repeats; no declared occurrence maximum | Path | `[]` |
| <a id="s-c322de089c"></a>`semantic_validator_provider`<br>`--semantic-validator-provider` | optional option; 1 value; collects repeats; no declared occurrence maximum | not recorded | `[]` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-3f7b8569a5"></a>`help` | <a id="s-d96f74d625"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-46223dbefe"></a>`0` | <a id="s-a225e25c21"></a>`"noncontractual-framework-help"` | <a id="s-838f158800"></a>`"empty"` |

### Result and failure contract

- <a id="s-2afc9660ff"></a>Result identity: `stove0-observer-conformance-cli-result/root/v1`
- <a id="s-c25f2c3b51"></a>Profile: `stove0-observer-conformance-cli/v1`
- <a id="s-e46691ab2c"></a>Structured output: `always-json`
- <a id="s-0fa93f9723"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1327368945"></a>`conformant` | <a id="s-5e5db3361b"></a>`{"kind":"conformance-completed"}` | <a id="s-c66cfdc7f4"></a>`0` | <a id="s-87e367e201"></a>json: [generated:stove0-observer: ObserverConformanceResult](../process-protocol-schemas/generated-stove0-observer-observerconformanceresult.md) | <a id="s-529648d0d7"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b34855a653"></a>`usage` | <a id="s-fe2ac3466e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-c5f5947482"></a>`2` | <a id="s-2442499427"></a>all: `"empty"` | <a id="s-95cc494653"></a>all: `"noncontractual-usage-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-observer-conformance"}; maximum=null; reason="no-declared-semantic-maximum"; source_constraint={"field":"kind"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --invocation](#s-3ce127cbc3) | `cardinality · occurrences · operational_policy` | shared above |
| [CLI parameter --semantic-vectors](#s-38ac0c67ea) | `cardinality · occurrences · operational_policy` | shared above |
| [CLI parameter --semantic-validator-provider](#s-c322de089c) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter 0](#s-6504128d72) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --invocation](#s-3ce127cbc3) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --semantic-vectors](#s-38ac0c67ea) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --semantic-validator-provider](#s-c322de089c) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-cb8af50470"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-997f1966d4"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-66e96d960e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-observer-conformance](../../../evidence/sources.md#src-5719d140a8) — [reference/stove0/packages/observer-support/src/stove0\_observer\_support/conformance.py::&lt;module&gt;](../../../../../../reference/stove0/packages/observer-support/src/stove0_observer_support/conformance.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0-observer-conformance/allow_abbrev`
- `/external_contract/cli/stove0-observer-conformance/name`
- `/external_contract/cli/stove0-observer-conformance/parameters`
- `/external_contract/cli/stove0-observer-conformance/result_contract`
- `/external_contract/cli/stove0-observer-conformance/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-observer-conformance/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0-observer-conformance/name`

<!-- exact-contract-value: e0b72013170271e2f13e91746ae3838974b2346286ccc7288e52e35f176d8677 -->

```json
"stove0-observer-conformance"
```

### `/external_contract/cli/stove0-observer-conformance/parameters`

<!-- exact-contract-value: 081d1ac083ce7b64aa00e86ad69c85cc777077719787c8513002aaf88d4360fe -->

```json
[
  {
    "dest": "base_url",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [],
    "required": true
  },
  {
    "default": [],
    "dest": "invocation",
    "kind": "_AppendAction",
    "nargs": null,
    "options": [
      "--invocation"
    ],
    "required": false,
    "type": "Path"
  },
  {
    "default": [],
    "dest": "semantic_vectors",
    "kind": "_AppendAction",
    "nargs": null,
    "options": [
      "--semantic-vectors"
    ],
    "required": false,
    "type": "Path"
  },
  {
    "default": [],
    "dest": "semantic_validator_provider",
    "kind": "_AppendAction",
    "nargs": null,
    "options": [
      "--semantic-validator-provider"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/stove0-observer-conformance/result_contract`

<!-- exact-contract-value: e6a14a46b7e96693a021712a07bc32d0d267b6fcd924f67cc832620f0fb506d4 -->

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
  "identity": "stove0-observer-conformance-cli-result/root/v1",
  "profile_id": "stove0-observer-conformance-cli/v1",
  "structured_output": "always-json",
  "success": [
    {
      "exit_status": 0,
      "id": "conformant",
      "selected_by": {
        "kind": "conformance-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": {
          "authority": "generated:stove0-observer",
          "definition": "ObserverConformanceResult",
          "kind": "schema-authority"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0-observer-conformance/terminating_controls`

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
