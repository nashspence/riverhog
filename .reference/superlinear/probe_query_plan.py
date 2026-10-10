"""Compare the same reachability query with explicit join order (diagnostic only)."""
import json
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent))
import probe_quadratic as p

original=p.method_sql(p.JSTORE,'_Edges','append','WITH RECURSIVE reachable')
ordered=original.replace('FROM edges JOIN reachable ON','FROM reachable CROSS JOIN edges ON')
rows=[]
for n in (128,256,512):
 db=p.make_db(p.ddl(p.JSTORE,'JournalStore'))
 plan=[r[3] for r in db.execute('EXPLAIN QUERY PLAN '+ordered,('a','derivation','b'))]
 def work():
  for i in reversed(range(n)):
   source,target=f'n{i:08}',f'n{i+1:08}'
   assert db.execute(ordered,(target,'derivation',source)).fetchone() is None
   db.execute('INSERT OR IGNORE INTO edges VALUES(?,?,?)',('derivation',source,target))
 result=p.measure_sql(db,work);result.update(n=n,plan=plan)
 assert db.execute(ordered,('n00000000','derivation',f'n{n:08}')).fetchone() is not None
 rows.append(result);db.close()
print(json.dumps(rows,indent=2))
Path(__file__).with_name('reachability_join_order.json').write_text(json.dumps({'sqlite':p.sqlite3.sqlite_version,'original_query':original,'experimental_query':ordered,'warning':'Reduces the measured query-plan amplification but retains repeated reachability; not a linear-time production fix.','rows':rows},indent=2)+'\n')
