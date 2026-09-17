# piggity local

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local:548007a7bb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-86205dda13"></a>Parser name: `local`

| Field | Value |
|---|---|
| <a id="s-2fefbd3385"></a>`parameters` | `[]` |
- <a id="s-eff45f2a6f"></a>Subcommand selection: required.
- <a id="s-d40b135f44"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-2381711a7a"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-97dbfb1dfd"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-1af395ab49"></a>`help` | <a id="s-4a862f2ca7"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-339ded6a19"></a>`0` | <a id="s-751b887f3a"></a>`"noncontractual-framework-help"` | <a id="s-42c4522cf0"></a>`"empty"` |
| <a id="s-92aa7e918a"></a>`implicit-help` | <a id="s-027bdf2192"></a>`{"kind":"empty-invocation"}` | <a id="s-f7fffb24b0"></a>`2` | <a id="s-4c4903e67a"></a>`"empty"` | <a id="s-595b6ff30c"></a>`"noncontractual-framework-help"` |

## Governing policies

- <a id="pa-7ff030ea0f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/piggity/commands/local/allow_extra_args`
- `/external_contract/cli/piggity/commands/local/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/local/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/local/name`
- `/external_contract/cli/piggity/commands/local/parameters`
- `/external_contract/cli/piggity/commands/local/subcommand_required`
- `/external_contract/cli/piggity/commands/local/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/local/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/name`

<!-- exact-contract-value: ea17e25c6c7aa6ef9407f976a670e87e37bc7e2a86b7bd4a3305bb16f1ba6052 -->

```json
"local"
```

### `/external_contract/cli/piggity/commands/local/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/local/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/local/terminating_controls`

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
