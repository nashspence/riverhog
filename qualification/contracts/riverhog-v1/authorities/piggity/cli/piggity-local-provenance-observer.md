# piggity local provenance-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-provenance-observer:b4e09cb22b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-008acffa9c"></a>Parser name: `provenance-observer`

| Field | Value |
|---|---|
| <a id="s-e4f1e23abb"></a>`parameters` | `[]` |
- <a id="s-fa7442b073"></a>Subcommand selection: required.
- <a id="s-c084649bef"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-8793d5fb27"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-f1f527d66b"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-fff542a7ae"></a>`help` | <a id="s-936e878003"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-957b9b4c56"></a>`0` | <a id="s-eb7338fa94"></a>`"noncontractual-framework-help"` | <a id="s-46a6e46711"></a>`"empty"` |
| <a id="s-4fca9d200b"></a>`implicit-help` | <a id="s-3b3a7b2631"></a>`{"kind":"empty-invocation"}` | <a id="s-6f2ae25895"></a>`2` | <a id="s-0bc1e58543"></a>`"empty"` | <a id="s-865afadb72"></a>`"noncontractual-framework-help"` |

## Governing policies

- <a id="pa-dfb55f7e21"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources/authorities.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/allow_extra_args`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/name`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/parameters`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/subcommand_required`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/name`

<!-- exact-contract-value: 9a274a23ecfd214275ad5e6ec65b7ddb001f01bdc958a7d41c955e9592ba5b40 -->

```json
"provenance-observer"
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/terminating_controls`

<!-- exact-contract-value: a96bedb6acb408986e7084c1d3216345e293f6c42987bb87a923c18807587f21 -->

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
        "--help"
      ]
    }
  },
  {
    "exit_status": 2,
    "id": "implicit-help",
    "stderr": "noncontractual-framework-help",
    "stdout": "empty",
    "trigger": {
      "kind": "empty-invocation"
    }
  }
]
```

</details>
