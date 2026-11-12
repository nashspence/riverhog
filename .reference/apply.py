from pathlib import Path
import subprocess, shutil
r=Path(__file__).resolve().parents[1]
BASE="dd09f4ab5e824935e680f26fba3d73d2484cb426"
changes=(
    "riverhog/src/riverhog_core/services/upload_journal_validation_retirement.py",
    "riverhog/src/riverhog_core/services/journal_retirement_registry.py",
    "riverhog/src/riverhog_core/services/workspace_retirement_registry.py",
    "riverhog/src/riverhog_core/services/physical_execution.py",
    "tests/unit/test_upload_journal_validation_retirement_loop_growth.py",
    "tests/support/journal_retirement.py",
    "tests/unit/test_upload_journal_service_scheduler_growth.py",
)
for name in changes:
    expected=subprocess.check_output(["git","show",BASE+":"+name],cwd=r)
    assert (r/name).read_bytes()==expected, "requires the exact audited unmodified source: "+name

p=r/'riverhog/src/riverhog_core/services/upload_journal_validation_retirement.py'
s=p.read_text(); start=s.index('def _retirement_loop_children()'); end=s.index('_PHASES =',start)
s=s[:start]+s[end:]
s=s.replace('''declare_growth(_retirement_loop_declaration(), delegates=_retirement_loop_children())(
    UploadJournalValidationRetirement.advance
)''','''from .journal_retirement_growth import declare_journal_retirement_growth


declare_journal_retirement_growth(UploadJournalValidationRetirement)
''')
s=s.replace('"""Schedule/receive bounded quanta without waiting for Native filesystem work."""','''"""Return scheduling bookkeeping events, not confirmed retirement progress.

        Each received physical outcome and each withdrawn ordinary reservation
        count separately. Close resubmissions and new submissions also count.
        For positive limit L the three phases return at most 2L, L and L.
        Resource and catalog work remains on the exact original worker.
        """''')
s=s.replace('from .journal_retirement_growth import declare_journal_retirement_growth\n\n\n', '')
s=s.replace('from .physical_execution import PhysicalExecutionOwner', 'from .physical_execution import PhysicalExecutionOwner\nfrom .journal_retirement_growth import declare_journal_retirement_growth')
p.write_text(s)
p=r/'riverhog/src/riverhog_core/services/journal_retirement_registry.py';s=p.read_text()
s=s.replace('class JournalRetirementTurn:', '''def format_retirement_blocker(error: BaseException) -> str:
    """Format the complete error on its worker before the existing detached slice."""
    return str(error)[:1000]


class JournalRetirementTurn:''')
s=s.replace('self.cleanup._blocked(self.job, str(error)[:1000])','self.cleanup._blocked(self.job, format_retirement_blocker(error))')
needle='    def _scope(\n'
i=s.index(needle,s.index('class JournalRetirementRegistry'))
s=s[:i]+'''    def collect(self, *, limit: int) -> int:
        return super().collect(limit=limit)

    def resume_ready(self, *, limit: int, retry_after) -> int:
        return super().resume_ready(limit=limit, retry_after=retry_after)

    def retained(self, workspace_id: str) -> bool:
        return super().retained(workspace_id)

    def submit(self, job, *, octet_budget, metadata_budget, retry_after) -> int:
        return super().submit(
            job, octet_budget=octet_budget, metadata_budget=metadata_budget,
            retry_after=retry_after,
        )

    def _run(self, context):
        return super()._run(context)

'''+s[i:]
p.write_text(s)
p=r/'riverhog/src/riverhog_core/services/physical_execution.py';s=p.read_text()
a=s.index('        pending = [error] if error is not None else []')
b=s.index('\n\n@dataclass',a)
body=s[a:b]
helper='\n\ndef release_exception_graph(error: BaseException | None) -> None:\n    """Execution-only frame receiving; never a resource or worker receipt."""\n'
helper+='\n'.join(line[4:] if line.startswith('    ') else line for line in body.splitlines())
s=s[:a]+'        release_exception_graph(error)'+helper+s[b:]
p.write_text(s)
p=r/'riverhog/src/riverhog_core/services/workspace_retirement_registry.py';s=p.read_text()
s=s.replace('    PhysicalWorkspaceExecutionKey,','    PhysicalWorkspaceExecutionKey,\n    release_exception_graph,')
s=s.replace('@dataclass(slots=True)\nclass _Context','''@dataclass(frozen=True, slots=True)
class _ReceivingPending:
    """A joined close attempt is not a positive resource/diagnostic receipt."""

    error_type: str


@dataclass(slots=True)
class _Context''')
s=s.replace('    resume_after: float = 0','    pending_diagnostic: BaseException | None = None\n    diagnostic_failure: BaseException | None = None\n    resume_after: float = 0')
s=s.replace('                    if isinstance(result, _Closed):', '                    if isinstance(result, _ReceivingPending):\n                        if not context.closing or context.closed:\n                            raise Conflict("pending receiving lacks its original context")\n                    elif isinstance(result, _Closed):')
s=s.replace('                    context.closing = True\n                    if context.unknown is None:','                    context.closing = True\n                    context.closed = False\n                    if context.unknown is None:')
s=s.replace('                finally:\n                    if context.closed:\n                        outcome.release_exception_frames()\n                        if context.unknown is not None:\n                            context.unknown.release_exception_frames()\n                            context.unknown = None\n','')
s=s.replace('                context.actor.close()\n                if not context.actor.received:\n                    raise Conflict("original workspace receiving remains unconfirmed")\n                context.closed = True\n                return _Closed()','                # Retain secondary receiving exceptions before frame disposal.\n                if context.pending_diagnostic is not None:\n                    try:\n                        release_exception_graph(context.pending_diagnostic)\n                    except BaseException as error:\n                        context.diagnostic_failure = error\n                        return _ReceivingPending(type(error).__name__)\n                    context.pending_diagnostic = None\n                    context.diagnostic_failure = None\n                try:\n                    context.actor.close()\n                    if not context.actor.received:\n                        raise Conflict("original workspace receiving remains unconfirmed")\n                    if context.unknown is not None:\n                        context.unknown.release_exception_frames()\n                        context.unknown = None\n                except BaseException as error:\n                    context.pending_diagnostic = error\n                    try:\n                        release_exception_graph(error)\n                    except BaseException as failure:\n                        context.diagnostic_failure = failure\n                    else:\n                        context.pending_diagnostic = None\n                        context.diagnostic_failure = None\n                    return _ReceivingPending(type(error).__name__)\n                context.closed = True\n                return _Closed()')
s=s.replace('    def _run(self, context: _Context[JobT, ProgressT]) -> ProgressT | _Closed:',
            '    def _run(self, context: _Context[JobT, ProgressT]) -> ProgressT | _Closed | _ReceivingPending:')
a=s.index('    def receive_stopped(self)')
head,tail=s[:a],s[a:]
tail=tail.replace('                context.closed = True',
    '                if context.pending_diagnostic is not None:\n                    release_exception_graph(context.pending_diagnostic)\n                    context.pending_diagnostic = None\n                    context.diagnostic_failure = None\n                context.closed = True')
s=head+tail
p.write_text(s)

for source,target in {
    "journal_retirement_growth.py":"riverhog/src/riverhog_core/services/journal_retirement_growth.py",
    "async_retirement_growth.py":"tests/support/async_retirement_growth.py",
    "test_retirement_loop.py":"tests/unit/test_upload_journal_validation_retirement_loop_growth.py",
    "test_phase_edges.py":"tests/unit/test_async_retirement_phase_edges.py",
}.items():
    shutil.copyfile(r/".reference"/source,r/target)

p=r/"tests/support/journal_retirement.py"
s=p.read_text().replace("def make(case, *, metadata_budget=1024 * 1024):", "def make(case, *, metadata_budget=1024 * 1024, terminal_workers=2):")
s=s.replace("pool = owned.make(terminal_workers=2)", "pool = owned.make(terminal_workers=terminal_workers)")
p.write_text(s)

# Parent scheduling ends before original workers are released or joined.
p=r/'tests/unit/test_upload_journal_service_scheduler_growth.py'
s=p.read_text()
s=s.replace('ROOT = owner.SqlAlchemyCollectionUploadService',
    'from tests.support.async_retirement_growth import PhaseEvidence, REGISTRY\n\n_ASYNC_WORKERS = []\n_ASYNC_TRANSCRIPTS = []\n\nROOT = owner.SqlAlchemyCollectionUploadService')
s=s.replace('self.case, selected, limit=limit, budget=octet_budget, cold=False',
    'self.case, selected, limit=limit, budget=octet_budget, cold=False, evidence=self.retirement_evidence')
s=s.replace('    def measure(self, *, limit=1, interrupt=False, expected_error=None):',
"""    def measure(self, *, limit=1, interrupt=False, expected_error=None):
        cleanup=self.current._journal_validation_retirement
        original=SimpleNamespace(cleanup=cleanup, registry=cleanup._registry,
            pool=cleanup._registry._execution, ledger=cleanup._registry._retention)
        self.retirement_evidence=PhaseEvidence(self.case,original)
        with self.retirement_evidence.installed(install_schedule=False) as evidence:
            try:
                result=self._measure(limit=limit,interrupt=interrupt,expected_error=expected_error)
            finally:
                evidence.drain()
        _ASYNC_WORKERS.extend(sample for op,sample,_ in evidence.roots if op is REGISTRY['_run'])
        _ASYNC_TRANSCRIPTS.append(evidence.transcript())
        return result

    def _measure(self, *, limit=1, interrupt=False, expected_error=None):""")
s=s.replace('assert result == 2 and observed["metrics"]["retirement_calls"] == 2',
    'assert result == 1 and observed["metrics"]["retirement_calls"] == 2\n        # Original join is outside ROOT; receive its bookkeeping on the next call.\n        samples.append(observed)\n        result, observed = recorder.measure(limit=2)\n        assert result == 1 and observed["metrics"]["retirement_calls"] == 2')
for shape in ('small','busy','rejection'):
    old=f'    record_property("growth_samples", {shape}_samples(tmp_path))'
    new=f'    _ASYNC_WORKERS.clear(); _ASYNC_TRANSCRIPTS.clear()\n{old}\n    record_property("separate_original_worker_samples", list(_ASYNC_WORKERS))\n    record_property("retirement_transcripts", list(_ASYNC_TRANSCRIPTS))'
    assert old in s
    s=s.replace(old,new)
p.write_text(s)

# Complete the concrete input modules' asynchronous declarations and vectors.
p=r/'riverhog/src/riverhog_core/services/journal_retirement_growth.py'
s=p.read_text()
insert='def mappings(steps):\n    return (\n        upper("context_mapping_steps", steps),\n        upper("context_enumerations", c(0)),\n        upper("context_key_octets", Expression("product", terms=(\n            c(32, (("count", -1), ("octets", 1))), steps)), OCTETS,\n            "Exact 32-character ASCII workspace keys; hash internals are separate."),\n    )\n\n\n'
s=s.replace('def declare_journal_retirement_growth(cleanup):',insert+'def declare_journal_retirement_growth(cleanup):')
s=s.replace('upper("lookups", c(1)), upper("enumerations", c(0)),','upper("lookups", c(1)), upper("enumerations", c(0)), *mappings(c(1)),')
s=s.replace('upper("actor_close_calls", c(0)),\n    ), children=(("physical", Execution.collect_workspace)', 'upper("actor_close_calls", c(0)), *mappings(add(scale(4, d("limit")), c(1))),\n    ), children=(("physical", Execution.collect_workspace)')
s=s.replace('upper("actor_close_calls", c(0)),\n    ), children=(("physical", Execution.try_submit_workspace)', 'upper("actor_close_calls", c(0)), *mappings(add(scale(2, d("limit")), c(1))),\n    ), children=(("physical", Execution.try_submit_workspace)')
s=s.replace('upper("actor_creations", c(0)), upper("status_calls", c(1)),','upper("actor_creations", c(0)), upper("status_calls", c(1)), *mappings(c(2)),')
p.write_text(s)
p=r/'tests/unit/test_upload_journal_validation_retirement_loop_growth.py'
s=p.read_text();a=s.index('def measure(');b=s.index('\ndef loop_samples',a)
s=s[:a]+'def measure(case, current, *, limit, budget, cold, evidence):\n    before=len(evidence.roots)\n    result=evidence.wrapper(SCHEDULE)(current,limit=limit,octet_budget=budget)\n    return result,next(row for op,row,_ in evidence.roots[before:] if op is SCHEDULE)\n\n'+s[b:]
s=s.replace('    worker_samples = []','    worker_samples = []\n    transcripts = []',1)
s=s.replace('original = cleanups(case)','original = cleanups(case, terminal_workers=3 if kind=="three-busy" else 2)')
s=s.replace('                        progress = [context for context in evidence.links]',
    '                        transcripts.append({"case":index,"cold":cold,"kind":kind,"limit":limit,"budget":budget,**evidence.transcript()})')
s=s.replace('    return worker_samples','    return worker_samples, transcripts')
s=s.replace('workers=loop_samples(tmp_path,samples,cleanups)','workers,transcripts=loop_samples(tmp_path,samples,cleanups)')
s=s.replace('    record_property("separate_original_worker_samples",workers)','    record_property("separate_original_worker_samples",workers)\n    record_property("retirement_transcripts",transcripts)')
p.write_text(s)
p=r/'tests/unit/test_async_retirement_phase_edges.py'
s=p.read_text()
s=s.replace('tmp_path, cleanups):','tmp_path, cleanups, record_property):',1)
s=s.replace('tmp_path, cleanups, monkeypatch):','tmp_path, cleanups, monkeypatch, record_property):',1)
s=s.replace('tmp_path,cleanups,monkeypatch):\n    with native_case(tmp_path) as case:\n        f=cleanups(case)\n        create=',
    'tmp_path,cleanups,monkeypatch,record_property):\n    with native_case(tmp_path) as case:\n        f=cleanups(case)\n        create=')
s=s.replace('        assert len(e.links)==2','        assert len(e.links)==2\n        e.publish(record_property)')
s=s.replace('        assert close_roots[0]["delegates"]["frames"]','        assert close_roots[0]["delegates"]["frames"]\n        e.publish(record_property)')
s+='        e.publish(record_property)\n'
p.write_text(s)
