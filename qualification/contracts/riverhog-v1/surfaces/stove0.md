# Stove0

[Atlas](../index.md)

Stove0 is a maintainer-selected, nonnormative Riverhog reference application. It owns its interfaces and state without becoming Riverhog authority.

## Surface shape

| Semantic area | Exact authorities | Contract elements |
|---|---:|---:|
| Application | 7 | 249 |
| Observers | 9 | 22 |
| Targets | 10 | 23 |
| Review | 14 | 22 |
| Recipes | 2 | 3 |

## Application

Authorities: **7** · Contract elements: **249**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [stove0](../authorities/stove0/index.md) | 240 | cli, http, operation | Optional nonnormative command-line client for the Stove0 reference application. Optional nonnormative content-opaque transformation reference application for Riverhog. |
| [stove0-api-client](../authorities/stove0-api-client/index.md) | 2 | boundary, python | Official Python client for the stove0 v1 workflow API. |
| [stove0-client](../authorities/stove0-client/index.md) | 1 | boundary | Optional nonnormative command-line client for the Stove0 reference application. |
| [stove0-control](../authorities/stove0-control/index.md) | 1 | durable-state | Optional nonnormative content-opaque transformation reference application for Riverhog. |
| [stove0-operator-contracts](../authorities/stove0-operator-contracts/index.md) | 2 | boundary, python | Canonical public state contracts for the Stove0 v1 operator surface. |
| [stove0-protocol](../authorities/stove0-protocol/index.md) | 2 | boundary, python | Canonical content-opaque collection orchestration contracts for stove0. |
| [stove0-server](../authorities/stove0-server/index.md) | 1 | boundary | Optional nonnormative content-opaque transformation reference application for Riverhog. |

## Observers

Authorities: **9** · Contract elements: **22**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [stove0-exiftool-observer](../authorities/stove0-exiftool-observer/index.md) | 1 | boundary | Optional nonnormative ExifTool observer reference for Stove0. |
| [stove0-ffprobe-sampling-observer](../authorities/stove0-ffprobe-sampling-observer/index.md) | 1 | boundary | Optional nonnormative FFprobe sampling-observer reference for Stove0. |
| [stove0-media-metadata-observer-contracts](../authorities/stove0-media-metadata-observer-contracts/index.md) | 1 | boundary | Optional nonnormative media-metadata contract reference for Stove0 observers. |
| [stove0-media-sampling-observer-contracts](../authorities/stove0-media-sampling-observer-contracts/index.md) | 1 | boundary | Optional nonnormative media-sampling contract reference for Stove0 observers. |
| [stove0-observer-client](../authorities/stove0-observer-client/index.md) | 3 | boundary, python | Narrow HTTP client for Stove0 content observers. |
| [stove0-observer-conformance](../authorities/stove0-observer-conformance/index.md) | 1 | cli | External-author protocol, runtime, and conformance support for stove0 content observers. |
| [stove0-observer-protocol](../authorities/stove0-observer-protocol/index.md) | 3 | boundary, python | Dependency-light public contracts for external stove0 content observers. |
| [stove0-observer-schemas](../authorities/stove0-observer-schemas/index.md) | 1 | cli | External-author protocol, runtime, and conformance support for stove0 content observers. |
| [stove0-observer-support](../authorities/stove0-observer-support/index.md) | 10 | boundary, protocol, python | External-author protocol, runtime, and conformance support for stove0 content observers. |

## Targets

Authorities: **10** · Contract elements: **23**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [stove0-media-archive-target-contracts](../authorities/stove0-media-archive-target-contracts/index.md) | 1 | boundary | Optional nonnormative media-archive contract reference for Stove0 targets. |
| [stove0-media-archive-target-support](../authorities/stove0-media-archive-target-support/index.md) | 1 | boundary | Optional nonnormative projection support for Stove0 media-archive references. |
| [stove0-nvenc-av1-opus-target](../authorities/stove0-nvenc-av1-opus-target/index.md) | 1 | boundary | Optional nonnormative NVENC AV1 and Opus target reference for Stove0. |
| [stove0-opus-target](../authorities/stove0-opus-target/index.md) | 1 | boundary | Optional nonnormative Opus target reference for Stove0. |
| [stove0-target-client](../authorities/stove0-target-client/index.md) | 2 | boundary, python | Narrow HTTP client for Stove0 transform targets. |
| [stove0-target-conformance](../authorities/stove0-target-conformance/index.md) | 1 | cli | Hardware-neutral target protocol, runtime, and conformance support for stove0. |
| [stove0-target-jobs](../authorities/stove0-target-jobs/index.md) | 1 | durable-state | Hardware-neutral target protocol, runtime, and conformance support for stove0. |
| [stove0-target-protocol](../authorities/stove0-target-protocol/index.md) | 3 | boundary, python | Dependency-light public contracts for external stove0 targets. |
| [stove0-target-schemas](../authorities/stove0-target-schemas/index.md) | 1 | cli | Hardware-neutral target protocol, runtime, and conformance support for stove0. |
| [stove0-target-support](../authorities/stove0-target-support/index.md) | 11 | boundary, protocol, python | Hardware-neutral target protocol, runtime, and conformance support for stove0. |

## Review

Authorities: **14** · Contract elements: **22**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [stove0-nvenc-av1-opus-review-sampler](../authorities/stove0-nvenc-av1-opus-review-sampler/index.md) | 1 | boundary | Optional nonnormative NVENC AV1 and Opus review-sampler reference for Stove0. |
| [stove0-opus-review-sampler](../authorities/stove0-opus-review-sampler/index.md) | 1 | boundary | Optional nonnormative Opus review-sampler reference for Stove0. |
| [stove0-review-materialize-target](../authorities/stove0-review-materialize-target/index.md) | 1 | boundary | Optional nonnormative review materialization target reference for Stove0. |
| [stove0-review-planning](../authorities/stove0-review-planning/index.md) | 2 | boundary, cli | Optional nonnormative planning bridge for maintained Stove0 review references. |
| [stove0-review-rclone-effect-target](../authorities/stove0-review-rclone-effect-target/index.md) | 1 | boundary | Optional nonnormative rclone review-effect target reference for Stove0. |
| [stove0-review-sampler-client](../authorities/stove0-review-sampler-client/index.md) | 1 | boundary | Optional nonnormative sampler-client reference for the Stove0 review target. |
| [stove0-review-sampler-conformance](../authorities/stove0-review-sampler-conformance/index.md) | 1 | cli | Optional nonnormative sampler support for Stove0 review references. |
| [stove0-review-sampler-protocol](../authorities/stove0-review-sampler-protocol/index.md) | 2 | boundary | Optional nonnormative sampler-protocol reference for the Stove0 review target. |
| [stove0-review-sampler-schemas](../authorities/stove0-review-sampler-schemas/index.md) | 1 | cli | Optional nonnormative sampler support for Stove0 review references. |
| [stove0-review-sampler-support](../authorities/stove0-review-sampler-support/index.md) | 7 | boundary, protocol | Optional nonnormative sampler support for Stove0 review references. |
| [stove0-review-target](../authorities/stove0-review-target/index.md) | 1 | configuration | Optional nonnormative shared review-target support reference for Stove0. |
| [stove0-review-target-contracts](../authorities/stove0-review-target-contracts/index.md) | 1 | boundary | Optional nonnormative review contract reference for Stove0 targets. |
| [stove0-review-target-sampler](../authorities/stove0-review-target-sampler/index.md) | 1 | configuration | Optional nonnormative shared review-target support reference for Stove0. |
| [stove0-review-target-support](../authorities/stove0-review-target-support/index.md) | 1 | boundary | Optional nonnormative shared review-target support reference for Stove0. |

## Recipes

Authorities: **2** · Contract elements: **3**

| Exact authority | Contract elements | Interfaces | Maintained purpose |
|---|---:|---|---|
| [stove0-recipe-config](../authorities/stove0-recipe-config/index.md) | 2 | boundary, python | Portable deployment-owned Stove0 recipe catalog contracts and validation. |
| [stove0-recipes](../authorities/stove0-recipes/index.md) | 1 | configuration | Portable deployment-owned Stove0 recipe catalog contracts and validation. |
