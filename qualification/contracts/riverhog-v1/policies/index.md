# V1 contract policies

[Atlas](../index.md)

Policies are defined once here and referenced from every dossier where they are proven to apply. Implementation-correctness witnesses remain outside this contract freeze artifact.

## Boundary

### `boundary/frozen-authority/v1`

The authority and extension boundary is maintainer-frozen for v1.

Applications: **85**

## Compatibility

### `compatibility/archive/v1`

A valid v1 archive remains independently recoverable throughout the supported v1 lifetime. Pack construction, ingress checkpointing, and resumable-write strategies are not archive-format compatibility promises.

Applications: **1**
### `compatibility/cli/v1`

Command names, options, exit status, and structured output remain backward compatible throughout v1; prose output is for people.

Applications: **315**
### `compatibility/components/v1`

A supported deployment runs components from one coordinated product version.

Applications: **240**
### `compatibility/configuration/v1`

Accepted v1 configuration remains valid throughout v1 unless an unsafe value must be rejected.

Applications: **127**
### `compatibility/http-api/v1`

Published v1 HTTP and CloudEvents contracts remain backward compatible throughout v1.

Applications: **692**
### `compatibility/python-api/v1`

Reusable-library declared public-module exports and public signatures remain backward compatible throughout v1; undeclared implementation submodules and other component roles are not Python API promises.

Applications: **26**
### `compatibility/recovery/v1`

Every later v1 recovery release reads every valid earlier v1 archive and provenance set.

Applications: **1**

## Extent Principles

### `extent-principle/bounded-work/v1`

Large logical totals cross bounded pages, segments, or restartable work steps; a carrier bound does not redefine the logical total.

Applications: **1**
### `extent-principle/configuration/v1`

Hardware- or environment-dependent limits that observably affect accepted work are operator-configurable and source-linked.

Applications: **1**
### `extent-principle/implementation-privacy/v1`

Buffers, provider mechanics, database layout, and other non-observable implementation extents are not frozen here.

Applications: **1**
### `extent-principle/logical-totals/v1`

A finite logical total has no product-level semantic maximum unless its owning contract declares one.

Applications: **4**
### `extent-principle/operational-capacity/v1`

Capacity policy may explicitly reject, defer, or throttle work, but must not silently truncate it or become an undocumented semantic ceiling.

Applications: **1**

## Extent Rules

### `extent-rule/bounded-segment/v1`

| Rule field | Value |
|---|---|
| `authority` | the owning schema's x-riverhog-extent declaration |
| `completion` | the owner-declared progression or repeated-work contract |
| `exceeded` | bounded-carrier-validation-error |
| `policy` | segmented_no_total_max |
| `semantic_maximum` | None |

Applications: **15**
### `extent-rule/configuration-composition/v1`

| Rule field | Value |
|---|---|
| `authority` | the owning validated deployment configuration document |
| `declared_operational_maximum` | None |
| `hidden_maximum` | forbidden |
| `policy` | operational_policy |
| `semantic_maximum` | None |
| `silent_truncation` | forbidden |

Applications: **6**
### `extent-rule/configured-capacity/v1`

| Rule field | Value |
|---|---|
| `authority` | the source-linked operator configuration field |
| `capacity_behavior` | explicit-reject-defer-or-throttle |
| `policy` | operational_policy |
| `silent_truncation` | forbidden |

Applications: **51**
### `extent-rule/extension-contract/v1`

| Rule field | Value |
|---|---|
| `authority` | the independently versioned extension contract |
| `core_semantic_maximum` | None |
| `policy` | extension_owned |

Applications: **17**
### `extent-rule/no-semantic-maximum/v1`

| Rule field | Value |
|---|---|
| `authority` | the owning schema's deliberate absence of a semantic maximum |
| `declared_operational_maximum` | None |
| `future_capacity_behavior` | explicit-configured-reject-defer-or-throttle |
| `hidden_maximum` | forbidden |
| `policy` | operational_policy |
| `semantic_maximum` | None |
| `silent_truncation` | forbidden |

Applications: **166**
### `extent-rule/route-progression/v1`

| Rule field | Value |
|---|---|
| `authority` | the route-owned x-riverhog-read-collection declaration |
| `completion` | the owning progression contract |
| `policy` | segmented_no_total_max |

Applications: **67**
### `extent-rule/schema-bound/v1`

| Rule field | Value |
|---|---|
| `authority` | the projected JSON Schema constraint |
| `exceeded` | schema-validation-error |
| `policy` | fixed-or-contract-max |
| `requirement` | a non-fixed set maximum carries an owning reason declaration |

Applications: **453**

## Exclusion

### `exclusion/process-launcher-not-cli/v1`

The installed entry point starts a separately inventoried process protocol and does not expose an independently maintained human or JSON CLI.

Applications: **13**
