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
