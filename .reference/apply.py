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
# Only replace the call site, not the extracted helper.
s=s.replace('self.cleanup._blocked(self.job, str(error)[:1000])','self.cleanup._blocked(self.job, format_retirement_blocker(error))')
# Specialize only the declaration binding of the shared controller; execution stays shared.
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
s=s.replace('                context.actor.close()\n                if not context.actor.received:\n                    raise Conflict("original workspace receiving remains unconfirmed")\n                context.closed = True\n                return _Closed()','                # The original actor keeps every resource obligation. Secondary\n                # receiving exceptions are retained before frame disposal, which\n                # stays on this worker and never grants resource receiving.\n                if context.pending_diagnostic is not None:\n                    try:\n                        release_exception_graph(context.pending_diagnostic)\n                    except BaseException as error:\n                        context.diagnostic_failure = error\n                        return _ReceivingPending(type(error).__name__)\n                    context.pending_diagnostic = None\n                    context.diagnostic_failure = None\n                try:\n                    context.actor.close()\n                    if not context.actor.received:\n                        raise Conflict("original workspace receiving remains unconfirmed")\n                    if context.unknown is not None:\n                        context.unknown.release_exception_frames()\n                        context.unknown = None\n                except BaseException as error:\n                    context.pending_diagnostic = error\n                    try:\n                        release_exception_graph(error)\n                    except BaseException as failure:\n                        context.diagnostic_failure = failure\n                    else:\n                        context.pending_diagnostic = None\n                        context.diagnostic_failure = None\n                    return _ReceivingPending(type(error).__name__)\n                context.closed = True\n                return _Closed()')
# Shutdown remains an explicit execution phase after actual joins.
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
