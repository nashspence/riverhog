"""Lifecycle costs around Native's separately declared corpus algorithms."""

from __future__ import annotations

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    SCALAR,
    Bound,
    Declaration,
    Dimension,
    Expression,
    Unit,
)


def _number(value: int, unit: Unit = COUNT) -> Expression:
    return Expression("constant", value=value, unit=unit)


def _dimension(name: str) -> Expression:
    return Expression("dimension", name=name)


def _scale(value: int, name: str) -> Expression:
    return Expression("product", terms=(_number(value, SCALAR), _dimension(name)))


def lifecycle_declaration(operation: str) -> Declaration:
    closing = operation == "close"
    opening = operation == "open_retained"
    dimensions = (
        Dimension("binding_octets", OCTETS, "Exact encoded immutable input binding width."),
        Dimension("checkpoint_octets", OCTETS, "Maximum complete before/after checkpoint width."),
        Dimension(
            "counter_octets", OCTETS, "Maximum complete decimal scalar width used by this call."
        ),
        Dimension(
            "retained_rows",
            COUNT,
            "Independent retained input and ordering rows, without enumeration.",
        ),
    )
    counters = Expression("sum", terms=(_scale(8, "counter_octets"), _number(128, OCTETS)))
    state_bytes = Expression("sum", terms=(_dimension("checkpoint_octets"), counters))
    parameters = Expression(
        "sum",
        terms=(
            _scale(4 if opening else 2, "binding_octets"),
            _scale(2, "checkpoint_octets"),
            counters,
        ),
    )
    limits = {
        "sql_statements": 0 if closing else 64 if opening else 7,
        "returned_rows": 0 if closing else 8 if opening else 3,
        "source_checks": 0 if closing else 2,
        "source_frame_reads": 0,
        "full_assertion_reads": 0,
        "closed_connections": 1 if closing or opening else 0,
        "opened_connections": 1 if opening else 0,
        "release_callbacks": 1 if closing else 0,
    }
    bounds = [
        Bound(metric, COUNT, "upper", _number(limit), meaning)
        for metric, limit, meaning in (
            (
                "sql_statements",
                limits["sql_statements"],
                "Fixed schema/lifecycle statements; transaction-body algorithms are separate.",
            ),
            (
                "returned_rows",
                limits["returned_rows"],
                "Only singleton lifecycle metadata is returned.",
            ),
            (
                "source_checks",
                limits["source_checks"],
                "Check the current owner before admission and commit.",
            ),
            (
                "source_frame_reads",
                0,
                "Lifecycle admission and checkpointing never replay source frames.",
            ),
            (
                "full_assertion_reads",
                0,
                "Lifecycle admission never reads documentary assertion payloads.",
            ),
            (
                "closed_connections",
                limits["closed_connections"],
                "Only the owned connection can close.",
            ),
            (
                "opened_connections",
                limits["opened_connections"],
                "At most one native connection is admitted.",
            ),
            (
                "release_callbacks",
                limits["release_callbacks"],
                "The transferred callback follows actual close.",
            ),
        )
    ]
    bounds.extend(
        (
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                _number(0, OCTETS) if closing else state_bytes,
                "One admitted checkpoint and its owning scalar metadata.",
            ),
            Bound(
                "parameter_octets",
                OCTETS,
                "upper",
                _number(0, OCTETS) if closing else parameters,
                "Complete repeated exact bindings, checkpoints and scalar compare-and-swap keys.",
            ),
            Bound(
                "checkpoint_decoded_octets",
                OCTETS,
                "upper",
                _number(0, OCTETS) if closing else _dimension("checkpoint_octets"),
                "Decode at most the admitted retained checkpoint, without traversing input data.",
            ),
            Bound(
                "checkpoint_encoded_octets",
                OCTETS,
                "upper",
                _number(0, OCTETS) if closing else _scale(2, "checkpoint_octets"),
                "Canonical round-trip admission and the one next checkpoint.",
            ),
        )
    )
    return Declaration(
        id="riverhog-provenance.retained-corpus-"
        + operation.strip("_").replace("_", "-")
        + ".growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.corpus_workspace.RetainedCorpusValidator." + operation,),
        scope="native retained corpus lifecycle"
        if opening or closing
        else "entry and exit of one retained corpus owner transaction, excluding its body",
        starts_at="receive the owned workspace, independent exact inputs and explicit worker fence"
        if opening
        else "enter the lifecycle boundary with current ownership",
        ends_at="return or reject the retained owner"
        if opening
        else "close its sole native handle"
        if closing
        else "commit or roll back the body and its parent checkpoint together",
        dimensions=dimensions,
        bounds=tuple(bounds),
        conditions=(
            "The application authenticates the workspace and holds its exclusive kernel lease.",
            "The source port performs bounded currentness and exact indexed membership checks.",
            "The supplied cached contract catalog owns its independent policy identity.",
            "State widths include all admitted metadata and the complete output checkpoint.",
        ),
        invariants=(
            "Only a strictly newer worker replaces a retained owner.",
            "Input binding, schema cookie, worker and checkpoint remain one atomic fence.",
            "Creation is exclusive; reopening does not create or repair missing state.",
            "Exact source/selection memberships and independent counts precede semantic freezing.",
            "Lifecycle work grants no member relevance, current archive access or publication.",
        ),
        limitations=(
            "Body algorithms, manifest membership and release callback internals need composition.",
            "SQL visits and metadata bytes do not establish I/O, lock-wait or total build bounds.",
            "These roots do not establish composed retained work or complete build conformance.",
        ),
    )


def order_declaration(native: Declaration, transaction: Declaration) -> Declaration:
    """Compose a retained quantum and charge its actual completed physical atoms."""

    def call(slot: str, declaration: Declaration, metric: str) -> Expression:
        return Expression(
            "delegate",
            name=slot,
            metric=metric,
            terms=(_dimension("native_calls") if slot == "native" else _number(1),),
            arguments=tuple((item.name, _dimension(item.name)) for item in declaration.dimensions),
        )

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def product(*terms: Expression) -> Expression:
        return Expression("product", terms=terms)

    atoms = add(_dimension("processed_atoms"), _number(1))
    sql = product(_number(32, SCALAR), atoms)
    widths = product(
        _number(9, SCALAR),
        add(
            _dimension("identity_octets"),
            _dimension("degree_octets"),
            _dimension("kind_octets"),
            _number(32, OCTETS_PER_COUNT),
        ),
    )
    parameters = add(call("transaction", transaction, "parameter_octets"), product(sql, widths))
    records = add(
        _dimension("retained_rows"), _dimension("edges"), _dimension("vertices"), _number(2)
    )
    vm = add(
        _number(512),
        product(
            atoms,
            add(
                _number(4096, SCALAR),
                product(_number(1024, SCALAR), Expression("log2_ceil", terms=(records,))),
            ),
        ),
    )
    decimal = product(
        _number(8, SCALAR), atoms, add(_dimension("degree_octets"), _number(1, OCTETS_PER_COUNT))
    )
    bounds = [
        Bound(
            "native_calls",
            COUNT,
            "upper",
            _number(1),
            "The retained operation invokes at most one admitted Native algorithm call.",
        ),
        Bound(
            "sql_statements",
            COUNT,
            "upper",
            add(
                call("transaction", transaction, "sql_statements"),
                call("native", native, "sql_statements"),
            ),
            "Actual lifecycle and Native body statement visits compose in the same database.",
        ),
        Bound(
            "algorithm_sql_statements",
            COUNT,
            "upper",
            sql,
            "At most one unfinished atom follows the independently counted completed atoms.",
        ),
        Bound(
            "sqlite_vm_instructions",
            COUNT,
            "upper",
            vm,
            "The existing Native SQLite B-tree allowance plus fixed singleton lifecycle work.",
        ),
        Bound(
            "returned_octets",
            OCTETS,
            "upper",
            add(call("transaction", transaction, "returned_octets"), product(sql, widths)),
            "Complete owner metadata, checkpoint, key, degree and length rows are charged.",
        ),
        Bound(
            "parameter_octets",
            OCTETS,
            "upper",
            parameters,
            "Full repeated scalar keys and exact parent/worker compare-and-swap preimages.",
        ),
        Bound(
            "written_rows",
            COUNT,
            "upper",
            add(_number(1), sql),
            "One parent publication and bounded singleton or exact-key Native row mutations.",
        ),
        Bound(
            "written_parameter_octets",
            OCTETS,
            "upper",
            parameters,
            "All parameters of successful mutating statements, including their exact fences.",
        ),
        Bound(
            "completed_phase_atoms",
            COUNT,
            "equal",
            _dimension("processed_atoms"),
            "Actual successful phase transitions agree with the returned progress count.",
        ),
        Bound(
            "admitted_phase_atoms",
            COUNT,
            "upper",
            call("native", native, "processed_rows"),
            "The caller's row admission independently bounds completed phase transitions.",
        ),
        Bound(
            "projected_payload_octets",
            OCTETS,
            "upper",
            call("native", native, "projected_payload_octets"),
            "The Native byte admission covers every ordering payload and its checkpoint.",
        ),
        Bound(
            "degree_decoded_octets",
            OCTETS,
            "upper",
            decimal,
            "Exact decimal degrees and three checkpoint counters, including vertices<=2E.",
        ),
        Bound(
            "degree_encoded_octets",
            OCTETS,
            "upper",
            decimal,
            "Canonical scalar writes use the same complete owning decimal widths.",
        ),
        Bound(
            "source_checks",
            COUNT,
            "upper",
            call("transaction", transaction, "source_checks"),
            "Current source checks occur before the transaction and before publication.",
        ),
        Bound(
            "checkpoint_decoded_octets",
            OCTETS,
            "upper",
            call("transaction", transaction, "checkpoint_decoded_octets"),
            "Only the admitted parent checkpoint is decoded.",
        ),
        Bound(
            "checkpoint_encoded_octets",
            OCTETS,
            "upper",
            call("transaction", transaction, "checkpoint_encoded_octets"),
            "Parent round-trip verification and publication retain their separate widths.",
        ),
    ]
    for metric in ("source_frame_reads", "full_assertion_reads"):
        bounds.append(
            Bound(
                metric,
                COUNT,
                "upper",
                add(call("transaction", transaction, metric), call("native", native, metric)),
                "Neither lifecycle nor cycle work loads original documentary payloads.",
            )
        )
    for metric in ("source_edge_rows", "outgoing_edge_rows", "seed_vertex_rows", "ready_rows"):
        bounds.append(
            Bound(
                metric,
                COUNT,
                "upper",
                atoms,
                "Each completed phase visits one key; an unfinished admission may revisit one key.",
            )
        )
    return Declaration(
        id="riverhog-provenance.retained-corpus-order-step.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.corpus_workspace.RetainedCorpusValidator.advance_order",),
        scope="one complete retained Native corpus cycle quantum with its current worker fence",
        starts_at="receive one frozen graph kind and explicit row/byte admission",
        ends_at="commit the exact Native work and parent checkpoint or roll back the quantum",
        dimensions=(
            *transaction.dimensions,
            *native.dimensions,
            Dimension(
                "native_calls",
                COUNT,
                "Actual Native algorithm invocations, zero on an earlier rejection.",
            ),
            Dimension(
                "processed_atoms",
                COUNT,
                "Completed phase atoms, independently observed before return or rollback.",
            ),
            Dimension(
                "edges", COUNT, "Independent distinct fixed input edges of the selected graph kind."
            ),
            Dimension(
                "vertices", COUNT, "Independent endpoint union of the selected fixed edge set."
            ),
            Dimension(
                "identity_octets",
                OCTETS_PER_COUNT,
                "Maximum complete UTF-8 endpoint identity width.",
            ),
            Dimension(
                "degree_octets", OCTETS_PER_COUNT, "Decimal width of max(1, independent edges)."
            ),
            Dimension("kind_octets", OCTETS_PER_COUNT, "Complete UTF-8 width of this graph kind."),
        ),
        bounds=tuple(bounds),
        conditions=(
            "Source/selection and documentary dependency terminals are complete and frozen.",
            "The owner authenticates its retained directory and holds its exclusive kernel lease.",
            "All rows share the current Native schema, B-tree keys and bounded cache policy.",
            "Widths and row extents cover all retained inputs, including unrelated "
            "graph-kind rows.",
            "Source-currentness callback internals retain their own owner composition.",
        ),
        invariants=(
            "Native indegree, ready queue and parent/worker checkpoints commit together.",
            "A cold acyclic run completes exactly 2E+3V+3 phase atoms; each "
            "advancing call completes at least one.",
            "Input/outgoing keys advance once; a byte boundary may repeat only its "
            "unfinished atom.",
            "Seed and ready keys each visit every vertex once across confirmed quanta.",
            "One completed graph kind grants no other semantic, member or access acceptance.",
        ),
        limitations=(
            "Failed bounded units may replay; completed units remain retained across replacement.",
            "Warm status requests have constant metadata cost and are separate "
            "from cold graph work.",
            "VM constants are engine dependent; internal key comparisons, I/O and "
            "lock waits are separate.",
            "Other corpus phases, application leases and complete discovery builds "
            "require composition.",
        ),
    )


def dependency_declaration(native: Declaration, transaction: Declaration) -> Declaration:
    """Charge one atomic dependency quantum, including all admitted metadata widths."""

    def call(slot: str, declaration: Declaration, metric: str) -> Expression:
        return Expression(
            "delegate",
            name=slot,
            metric=metric,
            terms=(_dimension("native_calls") if slot == "native" else _number(1),),
            arguments=tuple((item.name, _dimension(item.name)) for item in declaration.dimensions),
        )

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def product(*terms: Expression) -> Expression:
        return Expression("product", terms=terms)

    checks = _dimension("checked_obligations")
    probes = add(product(_number(3, SCALAR), checks), _number(1))
    projection_bytes = product(
        add(checks, _number(1)),
        add(_scale(16, "metadata_octets"), _number(256, OCTETS_PER_COUNT)),
    )
    parameters = add(
        call("transaction", transaction, "parameter_octets"),
        product(_number(5, SCALAR), probes, _dimension("metadata_octets")),
    )
    bounds = [
        Bound(
            "native_calls",
            COUNT,
            "upper",
            _number(1),
            "At most one Native dependency call follows lifecycle admission.",
        ),
        Bound(
            "sql_statements",
            COUNT,
            "upper",
            add(
                call("transaction", transaction, "sql_statements"),
                call("native", native, "sql_statements"),
            ),
            "All Native body and lifecycle statement visits occur in the same transaction.",
        ),
        Bound(
            "algorithm_sql_statements",
            COUNT,
            "upper",
            probes,
            "Each checked obligation has metadata, payload and target seeks; at most one "
            "unfinished or phase-exhaustion metadata seek follows.",
        ),
        Bound(
            "sqlite_vm_instructions",
            COUNT,
            "upper",
            add(
                _number(512),
                product(
                    add(checks, _number(1)),
                    add(
                        _number(4096, SCALAR),
                        product(
                            _number(1024, SCALAR),
                            Expression(
                                "log2_ceil", terms=(add(_dimension("retained_rows"), _number(2)),)
                            ),
                        ),
                    ),
                ),
            ),
            "Indexed Native point/continuation seeks plus singleton checkpoint work, under "
            "the maintained SQLite engine allowance.",
        ),
        Bound(
            "returned_rows",
            COUNT,
            "upper",
            add(call("transaction", transaction, "returned_rows"), probes),
            "Each Native statement returns at most one exact metadata or admitted payload row.",
        ),
        Bound(
            "returned_octets",
            OCTETS,
            "upper",
            add(
                call("transaction", transaction, "returned_octets"),
                projection_bytes,
                product(_number(3, SCALAR), probes, _dimension("metadata_octets")),
            ),
            "Complete admitted reference/fork/comparison payloads plus full scalar metadata "
            "and the retained parent checkpoint.",
        ),
        Bound(
            "parameter_octets",
            OCTETS,
            "upper",
            parameters,
            "Every repeated exact lookup coordinate and parent compare-and-swap preimage.",
        ),
        Bound(
            "written_rows",
            COUNT,
            "upper",
            _number(1),
            "The read-only Native dependency body writes no rows; the parent publishes once.",
        ),
        Bound(
            "written_parameter_octets",
            OCTETS,
            "upper",
            call("transaction", transaction, "parameter_octets"),
            "Only the owning parent checkpoint statement mutates persistent rows.",
        ),
        Bound(
            "completed_dependency_checks",
            COUNT,
            "equal",
            checks,
            "Independent successful exact-target checks agree with Native progress.",
        ),
        Bound(
            "processed_rows",
            COUNT,
            "upper",
            call("native", native, "processed_rows"),
            "The caller's row admission limits actual checked obligations.",
        ),
        Bound(
            "projected_payload_octets",
            OCTETS,
            "upper",
            call("native", native, "projected_payload_octets"),
            "Native length admission precedes every fetched dependency payload.",
        ),
        Bound(
            "json_decodes",
            COUNT,
            "upper",
            call("native", native, "json_decodes"),
            "Only admitted references and fork anchors are decoded by Native.",
        ),
        Bound(
            "dependency_decoded_octets",
            OCTETS,
            "upper",
            projection_bytes,
            "Native reference/anchor shapes have fixed fields and one complete entry preimage; "
            "decoded bytes also respect the separate Native payload admission.",
        ),
        Bound(
            "dependency_encoded_octets",
            OCTETS,
            "upper",
            product(checks, _dimension("metadata_octets")),
            "Each checked reference or fork re-encodes at most one complete original entry key.",
        ),
    ]
    for metric in ("source_checks", "checkpoint_decoded_octets", "checkpoint_encoded_octets"):
        bounds.append(
            Bound(
                metric,
                COUNT if metric == "source_checks" else OCTETS,
                "upper",
                call("transaction", transaction, metric),
                "Source currentness and checkpoint codecs retain their owning lifecycle cost.",
            )
        )
    for metric in ("source_frame_reads", "full_assertion_reads"):
        bounds.append(
            Bound(
                metric,
                COUNT,
                "upper",
                add(call("transaction", transaction, metric), call("native", native, metric)),
                "Neither lifecycle nor documentary dependency checking replays source payloads.",
            )
        )
    return Declaration(
        id="riverhog-provenance.retained-corpus-dependency-step.growth/v1",
        authority="riverhog-provenance",
        subjects=(
            "riverhog_provenance.corpus_workspace.RetainedCorpusValidator.advance_dependencies",
        ),
        scope="one complete retained Native documentary dependency quantum",
        starts_at="receive explicit row/byte admission under current fixed inputs and worker",
        ends_at="atomically publish Native continuation and parent checkpoint or roll back",
        dimensions=(
            *transaction.dimensions,
            *native.dimensions,
            Dimension("native_calls", COUNT, "Actual admitted Native algorithm invocations."),
            Dimension(
                "checked_obligations",
                COUNT,
                "Independent successful exact-target checks performed in this quantum.",
            ),
            Dimension(
                "metadata_octets",
                OCTETS_PER_COUNT,
                "Independent maximum full width of any retained Native lookup scalar or "
                "canonical entry-reference preimage, including counter and length widths.",
            ),
        ),
        bounds=tuple(bounds),
        conditions=(
            "Raw source indexing and fixed selected semantic registration are complete.",
            "The current application owner holds authenticated input custody and its kernel lease.",
            "Independent metadata widths cover complete scalar keys and entry preimages in all "
            "retained source rows, including unrelated rows.",
            "All queries use maintained Native schema indexes and the bounded SQLite cache.",
            "Native reference/anchor projections contain fixed identity fields and one "
            "canonical entry preimage; metadata_octets covers that entire preimage.",
            "Currentness callback internals retain their separate application-owned composition.",
        ),
        invariants=(
            "References precede forks, then size/SHA-256 comparisons, then both cycle checks.",
            "A cold complete pass makes exactly 3N+3 Native statement visits for N obligations.",
            "A capacity boundary retains at most one unfinished key, never a completed "
            "dependency under a confirmed parent checkpoint.",
            "Progress and all prior confirmed dependency checks survive worker replacement.",
            "Native documentary semantics, protected commitments and original coordinates remain.",
        ),
        limitations=(
            "A failed unconfirmed quantum may replay; this does not invalidate earlier commits.",
            "Warm Native status is separate from cold dependency work and still pays lifecycle.",
            "Engine VM constants exclude byte comparisons, storage I/O and lock waiting.",
            "Complete manifest coverage, cycles, member relevance and current access are separate.",
        ),
    )


def semantic_declaration(native: Declaration, transaction: Declaration) -> Declaration:
    """Compose selected-union registration with its independent manifest admission."""

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def product(*terms: Expression) -> Expression:
        return Expression("product", terms=terms)

    def call(slot: str, declaration: Declaration, metric: str) -> Expression:
        return Expression(
            "delegate",
            name=slot,
            metric=metric,
            terms=(_dimension("native_calls") if slot == "native" else _number(1),),
            arguments=tuple((item.name, _dimension(item.name)) for item in declaration.dimensions),
        )

    inputs = _dimension("processed_inputs")
    probes = add(product(_number(8, SCALAR), inputs), _number(2))
    manifest_bytes = _scale(2, "counter_octets")
    projection_bytes = product(
        add(inputs, _number(1)),
        add(_scale(8, "metadata_octets"), _number(128, OCTETS_PER_COUNT)),
    )
    parameters = add(
        call("transaction", transaction, "parameter_octets"),
        product(_number(5, SCALAR), probes, _dimension("metadata_octets")),
    )
    limits = (
        (
            "native_calls",
            COUNT,
            _number(1),
            "At most one Native call follows the independent source/selection terminal check.",
        ),
        (
            "input_count_checks",
            COUNT,
            _number(1),
            "One singleton count check precedes Native registration; it is not "
            "lifecycle body work.",
        ),
        (
            "sql_statements",
            COUNT,
            add(
                call("transaction", transaction, "sql_statements"),
                call("native", native, "sql_statements"),
                _number(1),
            ),
            "Lifecycle, Native body and the separate exact-input terminal query compose.",
        ),
        (
            "algorithm_sql_statements",
            COUNT,
            probes,
            "Each original assertion uses bounded metadata, interval, payload and obligation work; "
            "one unfinished atom may repeat its two metadata seeks.",
        ),
        (
            "sqlite_vm_instructions",
            COUNT,
            add(
                _number(512),
                product(
                    add(inputs, _number(1)),
                    add(
                        _number(4096, SCALAR),
                        product(
                            _number(1024, SCALAR),
                            Expression(
                                "log2_ceil", terms=(add(_dimension("retained_rows"), _number(2)),)
                            ),
                        ),
                    ),
                ),
            ),
            "Indexed input/interval/obligation access plus fixed manifest and checkpoint work.",
        ),
        (
            "returned_rows",
            COUNT,
            add(
                call("transaction", transaction, "returned_rows"),
                product(_number(3, SCALAR), inputs),
                _number(3),
            ),
            "At most three Native rows per input, two unfinished rows and one manifest row.",
        ),
        (
            "returned_octets",
            OCTETS,
            add(
                call("transaction", transaction, "returned_octets"),
                manifest_bytes,
                projection_bytes,
                product(_number(6, SCALAR), probes, _dimension("metadata_octets")),
            ),
            "Full projection bytes, six-scalar input metadata, exact interval keys "
            "and parent state.",
        ),
        (
            "parameter_octets",
            OCTETS,
            parameters,
            "All repeated exact keys, obligation scalars and parent compare-and-swap preimages.",
        ),
        (
            "written_rows",
            COUNT,
            add(_number(1), inputs),
            "At most one selected-union obligation per original assertion and one "
            "parent publication.",
        ),
        (
            "written_parameter_octets",
            OCTETS,
            add(
                call("transaction", transaction, "parameter_octets"),
                product(_number(5, SCALAR), inputs, _dimension("metadata_octets")),
            ),
            "Successful edge/comparison insert parameters and the complete parent "
            "checkpoint write.",
        ),
        (
            "processed_rows",
            COUNT,
            call("native", native, "processed_rows"),
            "Actual completed original inputs respect Native's row admission.",
        ),
        (
            "projected_payload_octets",
            OCTETS,
            call("native", native, "projected_payload_octets"),
            "Only admitted effective semantic projections are fetched.",
        ),
        (
            "json_decodes",
            COUNT,
            call("native", native, "json_decodes"),
            "Decode only the admitted selected projections, never full documentary assertions.",
        ),
        (
            "semantic_decoded_octets",
            OCTETS,
            projection_bytes,
            "Each Native projection has at most four scalar values and fixed JSON syntax; "
            "complete decoded bytes also respect the Native payload admission.",
        ),
    )
    bounds = [
        Bound(metric, unit, "upper", expression, meaning)
        for metric, unit, expression, meaning in limits
    ]
    bounds.append(
        Bound(
            "completed_semantic_inputs",
            COUNT,
            "equal",
            inputs,
            "Independently confirmed original input keys agree with Native progress "
            "and its cursor.",
        )
    )
    for metric in ("source_checks", "checkpoint_decoded_octets", "checkpoint_encoded_octets"):
        bounds.append(
            Bound(
                metric,
                COUNT if metric == "source_checks" else OCTETS,
                "upper",
                call("transaction", transaction, metric),
                "Currentness and parent checkpoint codecs retain their owning lifecycle cost.",
            )
        )
    for metric in ("source_frame_reads", "full_assertion_reads"):
        bounds.append(
            Bound(
                metric,
                COUNT,
                "upper",
                add(call("transaction", transaction, metric), call("native", native, metric)),
                "Neither the lifecycle nor selected-union registration replays original payloads.",
            )
        )
    return Declaration(
        id="riverhog-provenance.retained-corpus-semantic-step.growth/v1",
        authority="riverhog-provenance",
        subjects=(
            "riverhog_provenance.corpus_workspace.RetainedCorpusValidator.advance_semantics",
        ),
        scope="one atomic retained Native selected-source semantic union quantum",
        starts_at="receive admitted work under current ownership and fixed input expectations",
        ends_at="publish exact source-union obligations and parent checkpoint or roll back",
        dimensions=(
            *transaction.dimensions,
            *native.dimensions,
            Dimension(
                "native_calls", COUNT, "Actual Native algorithm invocations after admission."
            ),
            Dimension(
                "processed_inputs",
                COUNT,
                "Independently confirmed original semantic input keys in this call.",
            ),
            Dimension(
                "metadata_octets",
                OCTETS_PER_COUNT,
                "Independent maximum complete scalar width of retained Native identity, "
                "type, sequence, size, comparison result and endpoint keys.",
            ),
        ),
        bounds=tuple(bounds),
        conditions=(
            "Independent manifest membership and unique completion counts cover fixed inputs.",
            "The application owns authenticated source custody and its exclusive kernel lease.",
            "Native checks independent source/selection counts before freezing semantic inputs.",
            "Metadata widths and retained extents cover all indexed inputs and selected prefixes.",
            "All queries use the maintained Native schema indexes and bounded SQLite cache.",
            "Native projections carry at most four canonical ASCII identity/type/result scalars; "
            "unrelated documentary text is never part of a semantic projection.",
            "Currentness callback internals retain their separate application-owned composition.",
        ),
        invariants=(
            "Each original semantic assertion is considered once over the selected prefix union.",
            "An assertion contributes exactly when a selected prefix intersects "
            "[birth,retirement).",
            "A cold pass has at most 6A+2C+1 Native statement visits for A original inputs and "
            "C confirmed calls; advancing calls complete an input, so C<=A+1.",
            "Only an unfinished byte admission repeats metadata; confirmed inputs "
            "survive replacement.",
            "Native obligations and their parent continuation commit in one "
            "current-owner transaction.",
            "Source completion, dependencies, cycles, member relevance and access remain distinct.",
        ),
        limitations=(
            "Failed unconfirmed quanta may replay while prior confirmed work remains retained.",
            "Warm status retains constant manifest and lifecycle cost without Native input reads.",
            "VM constants exclude scalar comparisons, storage I/O and lock waiting.",
            "Raw-source admission, final source-normal commitments and publication "
            "require composition.",
        ),
    )


def point_declaration(operation: str, native: Declaration, transaction: Declaration) -> Declaration:
    """Charge the complete retained transaction around one exact Native lookup."""

    def call(slot: str, declaration: Declaration, metric: str) -> Expression:
        return Expression(
            "delegate",
            name=slot,
            metric=metric,
            terms=(_dimension("native_calls") if slot == "native" else _number(1),),
            arguments=tuple((item.name, _dimension(item.name)) for item in declaration.dimensions),
        )

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    limits = [
        Bound(
            "native_calls",
            COUNT,
            "upper",
            _number(1),
            "At most one exact Native lookup follows current owner admission.",
        ),
        Bound(
            "returned_octets",
            OCTETS,
            "upper",
            add(
                call("transaction", transaction, "returned_octets"),
                call("native", native, "metadata_octets"),
                call("native", native, "payload_octets"),
            ),
            "Complete singleton owner metadata, indexed point metadata and admitted payloads.",
        ),
        Bound(
            "sqlite_vm_instructions",
            COUNT,
            "upper",
            add(_number(512), call("native", native, "sqlite_vm_instructions")),
            "Native indexed seeks plus fixed singleton lifecycle and checkpoint work.",
        ),
        Bound(
            "written_rows",
            COUNT,
            "upper",
            _number(1),
            "The read-only Native body is followed by at most one parent checkpoint write.",
        ),
        Bound(
            "written_parameter_octets",
            OCTETS,
            "upper",
            call("transaction", transaction, "parameter_octets"),
            "All persistent writes belong to the owner checkpoint compare-and-swap.",
        ),
    ]
    for metric in ("sql_statements", "returned_rows", "parameter_octets"):
        limits.append(
            Bound(
                metric,
                OCTETS if metric.endswith("octets") else COUNT,
                "upper",
                add(call("transaction", transaction, metric), call("native", native, metric)),
                "Actual Native point work and owner entry/exit compose without omitting either.",
            )
        )
    for metric in ("source_checks", "checkpoint_decoded_octets", "checkpoint_encoded_octets"):
        limits.append(
            Bound(
                metric,
                COUNT if metric == "source_checks" else OCTETS,
                "upper",
                call("transaction", transaction, metric),
                "Current owner checks and complete checkpoint codecs retain lifecycle costs.",
            )
        )
    for metric in ("payload_octets", "decoded_octets", "json_decodes", "source_frame_reads"):
        limits.append(
            Bound(
                metric,
                OCTETS if metric.endswith("octets") else COUNT,
                "upper",
                call("native", native, metric),
                "The actual Native child owns all selected payload work; lifecycle adds none.",
            )
        )
    if operation == "selected_delivery_support":
        limits.append(
            Bound(
                "full_assertion_reads",
                COUNT,
                "equal",
                _number(0),
                "Delivery support reads only Native semantic projections and original entry keys.",
            )
        )
    return Declaration(
        id="riverhog-provenance.retained-corpus-" + operation.replace("_", "-") + ".growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.corpus_workspace.RetainedCorpusValidator." + operation,),
        scope="one current retained Native exact historical point lookup",
        starts_at="receive exact selection coordinates and explicit payload admission",
        ends_at="return detached original support and atomically publish the parent checkpoint",
        dimensions=(
            *transaction.dimensions,
            *native.dimensions,
            Dimension("native_calls", COUNT, "Actual admitted Native algorithm invocations."),
        ),
        bounds=tuple(limits),
        conditions=(
            "Raw source rows came from independently accepted immutable Native journals.",
            "The application authenticates the directory and holds its exclusive kernel lease.",
            "The current source binding and worker checkpoint remain the exact retained fence.",
            "Native length admission precedes every selected payload or projection fetch.",
            "Currentness callback internals retain their separate application-owned composition.",
        ),
        invariants=(
            "Exact source coordinates and half-open effective intervals remain Native-owned.",
            "Absent or retired objects do not scan earlier assertions or fall back to old values.",
            "Lookup results retain their original assertion and entry support coordinates.",
            "Failure rolls back the parent checkpoint without changing earlier confirmed work.",
            "A point result grants no global validation, member relevance or current access.",
        ),
        limitations=(
            "Source ingestion, fixed manifest coverage and corpus global terminals are separate.",
            "Each requested lookup pays its own bounded metadata and selected payload cost.",
            "SQLite VM allowances exclude byte comparisons, physical I/O and lock waiting.",
            "Complete application and discovery-build growth require further composition.",
        ),
    )


def selection_declaration(native: Declaration, transaction: Declaration) -> Declaration:
    """Compose exact prefix admission, Native selection and unique-input publication."""

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def product(*terms: Expression) -> Expression:
        return Expression("product", terms=terms)

    def call(slot: str, declaration: Declaration, metric: str) -> Expression:
        return Expression(
            "delegate",
            name=slot,
            metric=metric,
            terms=(_dimension("native_calls") if slot == "native" else _number(1),),
            arguments=tuple((item.name, _dimension(item.name)) for item in declaration.dimensions),
        )

    body_bytes = add(_scale(4, "anchor_octets"), _scale(4, "counter_octets"))
    bounds = [
        Bound(
            "native_calls",
            COUNT,
            "upper",
            _number(1),
            "At most one Native registration follows exact owned-manifest admission.",
        ),
        Bound(
            "manifest_checks",
            COUNT,
            "upper",
            _number(1),
            "One exact indexed selection membership callback precedes Native work.",
        ),
        Bound(
            "registration_statements",
            COUNT,
            "upper",
            _number(3),
            "One unique-input insert and conditional singleton count read and update.",
        ),
        Bound(
            "sql_statements",
            COUNT,
            "upper",
            add(
                call("transaction", transaction, "sql_statements"),
                call("native", native, "sql_statements"),
                _number(3),
            ),
            "Lifecycle, Native header/selection and distinct-input publication are separate costs.",
        ),
        Bound(
            "returned_rows",
            COUNT,
            "upper",
            add(call("transaction", transaction, "returned_rows"), _number(2)),
            "One exact Native header and at most one independent input-count scalar.",
        ),
        Bound(
            "returned_octets",
            OCTETS,
            "upper",
            add(
                call("transaction", transaction, "returned_octets"),
                _dimension("anchor_octets"),
                _dimension("counter_octets"),
            ),
            "Complete lifecycle metadata, exact source header and conditional count scalar.",
        ),
        Bound(
            "parameter_octets",
            OCTETS,
            "upper",
            add(call("transaction", transaction, "parameter_octets"), body_bytes),
            "Full repeated Native source coordinates, canonical membership key and count scalar.",
        ),
        Bound(
            "written_parameter_octets",
            OCTETS,
            "upper",
            add(call("transaction", transaction, "parameter_octets"), body_bytes),
            "All actual Native, unique-input, count and parent-checkpoint write parameters.",
        ),
        Bound(
            "written_rows",
            COUNT,
            "upper",
            _number(4),
            "At most one Native prefix, unique input, count update and parent checkpoint.",
        ),
        Bound(
            "selection_encoded_octets",
            OCTETS,
            "upper",
            _scale(2, "anchor_octets"),
            "One exact entry reference and one full canonical selected-input preimage.",
        ),
        Bound(
            "sqlite_vm_instructions",
            COUNT,
            "upper",
            add(
                _number(512),
                product(
                    _number(256),
                    Expression(
                        "log2_ceil",
                        terms=(
                            add(
                                _dimension("retained_rows"),
                                _dimension("selected_prefixes"),
                                _number(2),
                            ),
                        ),
                    ),
                ),
            ),
            "Indexed exact header and unique-key probes plus fixed singleton lifecycle work.",
        ),
    ]
    for metric in ("source_checks", "checkpoint_decoded_octets", "checkpoint_encoded_octets"):
        bounds.append(
            Bound(
                metric,
                COUNT if metric == "source_checks" else OCTETS,
                "upper",
                call("transaction", transaction, metric),
                "The current owner and complete checkpoint codecs retain lifecycle costs.",
            )
        )
    for metric in (
        "processed_rows",
        "projected_payload_octets",
        "json_decodes",
        "full_assertion_reads",
        "source_frame_reads",
    ):
        bounds.append(
            Bound(
                metric,
                OCTETS if metric.endswith("octets") else COUNT,
                "upper",
                call("native", native, metric),
                "Selection and parent registration load no original assertions or source frames.",
            )
        )
    return Declaration(
        id="riverhog-provenance.retained-corpus-select-source.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.corpus_workspace.RetainedCorpusValidator.select_source",),
        scope="one exact retained selected-prefix registration",
        starts_at="receive an exact independently expected selected Native anchor",
        ends_at="atomically publish Native selection, unique completion and parent checkpoint",
        dimensions=(
            *transaction.dimensions,
            *native.dimensions,
            Dimension("native_calls", COUNT, "Actual admitted Native selection calls."),
            Dimension("anchor_octets", OCTETS, "Complete canonical selected anchor width."),
        ),
        bounds=tuple(bounds),
        conditions=(
            "The current application owner authenticates the directory and holds its kernel lease.",
            "The source port owns independent fixed membership and expected unique counts.",
            "Native source rows retain every exact original anchor coordinate.",
            "Membership and currentness callback internals retain application-owned composition.",
            "Retained row extents and full before/after counter widths are independently known.",
        ),
        invariants=(
            "Exact manifest membership precedes Native selection work.",
            "Repeated completed inputs do not increment the independent unique-input count.",
            "Native selection, completion count and owner checkpoint commit in one transaction.",
            "Only independently complete source and selection counts permit semantic freezing.",
            "A selected prefix is not global Native, history, member or access acceptance.",
        ),
        limitations=(
            "Repeated explicit calls each pay bounded metadata work, including warm calls.",
            "Raw ingestion, semantic union, global checks and source-port internals are separate.",
            "SQLite VM allowances exclude physical I/O, byte comparisons and lock waiting.",
            "Complete discovery builds require further application-owned composition.",
        ),
    )


def source_declaration(
    native: Declaration, transaction: Declaration, header: Declaration
) -> Declaration:
    """Compose one owned raw-source quantum and its complete database boundary work."""

    def add(*terms: Expression) -> Expression:
        return Expression("sum", terms=terms)

    def product(*terms: Expression) -> Expression:
        return Expression("product", terms=terms)

    def call(slot: str, declaration: Declaration, metric: str) -> Expression:
        return Expression(
            "delegate",
            name=slot,
            metric=metric,
            terms=(_dimension("native_calls") if slot == "native" else _number(1),),
            arguments=tuple((item.name, _dimension(item.name)) for item in declaration.dimensions),
        )

    source_seeks = add(_scale(2, "new_frames"), _number(3))
    statements = add(
        call("transaction", transaction, "sql_statements"),
        call("native", native, "sql_statements"),
        source_seeks,
        _number(3),
    )
    metadata = product(statements, _dimension("source_metadata_octets"))
    parameters = add(
        call("transaction", transaction, "parameter_octets"),
        _scale(12, "entry_octets"),
        product(_number(8, SCALAR), metadata),
        _scale(4, "counter_octets"),
    )
    records = add(
        _dimension("retained_rows"),
        _dimension("retained_frames"),
        _dimension("new_frames"),
        _dimension("new_assertions"),
        _dimension("new_references"),
        _dimension("new_retractions"),
        _number(2),
    )
    bounds = [
        Bound(metric, COUNT, "upper", _number(limit), meaning)
        for metric, limit, meaning in (
            ("native_calls", 1, "One admitted Native source quantum follows exact membership."),
            ("manifest_checks", 1, "One exact source-manifest admission precedes Native work."),
            (
                "parent_header_calls",
                1,
                "An explicit prefix selects one independently accepted header.",
            ),
            (
                "registration_statements",
                3,
                "Unique completion uses one insert and a conditional count read/update.",
            ),
            (
                "full_retained_preimage_reads",
                0,
                "Stored identity and assertion preimages are not returned.",
            ),
        )
    ]
    bounds.extend(
        (
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                statements,
                "Count every lifecycle, Native, source-frame/header and completion statement.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                statements,
                "Each indexed statement detaches at most one row.",
            ),
            Bound(
                "returned_octets",
                OCTETS,
                "upper",
                add(
                    call("transaction", transaction, "returned_octets"),
                    _dimension("frame_octets"),
                    metadata,
                    _dimension("counter_octets"),
                ),
                "Original missing entry bytes, indexed metadata and owner checkpoint only.",
            ),
            Bound(
                "parameter_octets",
                OCTETS,
                "upper",
                parameters,
                "Entry/assertion candidates, exact keys, headers and owner CAS inputs.",
            ),
            Bound(
                "written_parameter_octets",
                OCTETS,
                "upper",
                parameters,
                "All inserted or updated row parameters remain within complete input accounting.",
            ),
            Bound(
                "written_rows",
                COUNT,
                "upper",
                statements,
                "Exact unique-key mutations affect at most one row per statement.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                add(
                    _number(1024),
                    product(
                        statements,
                        add(
                            _number(256, SCALAR),
                            product(
                                _number(128, SCALAR), Expression("log2_ceil", terms=(records,))
                            ),
                        ),
                    ),
                ),
                "Indexed seeks and per-entry mutations count every actual database instruction.",
            ),
            Bound(
                "canonical_encoded_octets",
                OCTETS,
                "upper",
                add(
                    call("transaction", transaction, "checkpoint_encoded_octets"),
                    _scale(8, "entry_octets"),
                    product(_number(8, SCALAR), metadata),
                ),
                "Native preimage, SHA state and parent metadata encodings.",
            ),
            Bound(
                "header_decoded_octets",
                OCTETS,
                "upper",
                product(_number(6, SCALAR), source_seeks, _dimension("source_metadata_octets")),
                "Decode only indexed references and SHA headers outside entry parsing.",
            ),
            Bound(
                "document_decoded_octets",
                OCTETS,
                "upper",
                _dimension("entry_octets"),
                "Decode each missing original Native entry once.",
            ),
        )
    )
    for metric in ("source_frame_reads", "source_frame_octets", "hashed_entry_octets"):
        bounds.append(
            Bound(
                metric,
                COUNT if metric == "source_frame_reads" else OCTETS,
                "upper",
                call("native", native, metric),
                "The parent's source admission and registration add no original-frame replay.",
            )
        )
    for metric in ("source_checks", "checkpoint_decoded_octets", "checkpoint_encoded_octets"):
        bounds.append(
            Bound(
                metric,
                COUNT if metric == "source_checks" else OCTETS,
                "upper",
                call("transaction", transaction, metric),
                "Retain complete current-owner and checkpoint lifecycle accounting.",
            )
        )
    return Declaration(
        id="riverhog-provenance.retained-corpus-advance-source.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.corpus_workspace.RetainedCorpusValidator.advance_source",),
        scope="one current-owner retained Native raw-source quantum",
        starts_at="receive an owned accepted snapshot and independently fixed source membership",
        ends_at="atomically retain or roll back raw rows, unique completion and checkpoint",
        dimensions=(
            *transaction.dimensions,
            *native.dimensions,
            *header.dimensions,
            Dimension("native_calls", COUNT, "Actual admitted Native source calls."),
            Dimension("parent_header_calls", COUNT, "Actual explicit parent-prefix selections."),
            Dimension(
                "source_metadata_octets",
                OCTETS_PER_COUNT,
                "Maximum complete accepted anchor/reference/SHA-state and source-key scalar width.",
            ),
        ),
        bounds=tuple(bounds),
        conditions=(
            "The application authenticates the workspace and holds its exclusive kernel lease.",
            "Independent source membership and unique counts precede raw indexing.",
            "Retained row extents include all participating Native indices, including new keys.",
            "Sources supply independently accepted bytes and complete indexed header widths.",
            "Entry parsing and canonical codecs retain the admitted Native atom policy.",
        ),
        invariants=(
            "Completed shared original prefixes are proved exactly and are never fetched again.",
            "Only complete unique source inputs increment the independent source count.",
            "Raw source rows, completion count and owner checkpoint share one transaction.",
            "Missing membership, ownership loss and failed obligations retain the old checkpoint.",
            "Raw completion does not discharge selection, global history or archive access.",
        ),
        limitations=(
            "Source-port membership/currentness internals remain application-owned composition.",
            "VM observations exclude internal byte comparisons, physical I/O and lock waiting.",
            "Canonical counters measure owner codec boundaries, excluding parser internals.",
            "Complete discovery and publication need application-owned composition.",
        ),
    )
