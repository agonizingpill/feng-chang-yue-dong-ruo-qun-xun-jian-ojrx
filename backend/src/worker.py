import os,json,time,traceback
from datetime import datetime
from pathlib import Path
import redis
from sqlalchemy import select,update
from .database import Session
from .models import Visit,Hive,ScreeningRun
from .screen_model import screen
from .yard_service import latest,push_run,RUN_STALE
queue=redis.Redis.from_url(os.getenv('REDIS_URL','redis://redis:6379/0'),decode_responses=True)

def sweep():
    cutoff=datetime.utcnow()-RUN_STALE
    with Session() as db:
        ids=db.scalars(select(ScreeningRun.id).where((ScreeningRun.status=='pending')|((ScreeningRun.status=='running')&(ScreeningRun.updated_at<cutoff)))).all()
    for run_id in ids:push_run(queue,run_id)

def claim(run_id):
    cutoff=datetime.utcnow()-RUN_STALE
    with Session() as db:
        row=db.execute(update(ScreeningRun).where(ScreeningRun.id==run_id,(ScreeningRun.status=='pending')|((ScreeningRun.status=='running')&(ScreeningRun.updated_at<cutoff))).values(status='running',attempt=ScreeningRun.attempt+1,updated_at=datetime.utcnow()).returning(ScreeningRun.attempt)).first()
        db.commit();return row[0] if row else None

def compute(run_id,attempt):
    with Session() as db:
        run=db.get(ScreeningRun,run_id)
        if run.points:return True
        found={v.id:v for v in db.scalars(select(Visit).where(Visit.id.in_(run.visit_ids))).all()}
        points=[];evidence=None
        for visit_id in run.visit_ids:
            visit=found.get(visit_id)
            if not visit:raise ValueError(f'巡检 {visit_id} 不存在，无法完成筛查')
            score,flag,margin,evidence=screen(visit)
            points.append({'visit_id':visit.id,'hive_id':visit.hive_id,'score':score,'flag':flag,'margin':margin})
        done=db.execute(update(ScreeningRun).where(ScreeningRun.id==run_id,ScreeningRun.status=='running',ScreeningRun.attempt==attempt).values(points=points,evidence=evidence,updated_at=datetime.utcnow()))
        db.commit();return done.rowcount==1

def publish(run_id,attempt):
    with Session() as db:
        run=db.scalar(select(ScreeningRun).where(ScreeningRun.id==run_id).with_for_update())
        if not run or run.status!='running' or run.attempt!=attempt:return 'stale'
        visits=db.scalars(select(Visit).where(Visit.id.in_(run.visit_ids)).order_by(Visit.id).with_for_update()).all()
        hives={h.id:h for h in db.scalars(select(Hive).where(Hive.id.in_(sorted({v.hive_id for v in visits}))).order_by(Hive.id).with_for_update()).all()}
        changed=sorted(hives[v.hive_id].code for v in visits if v.status!='screening' or latest(db,v.hive_id)!=v.id)
        if changed:
            run.status='voided';run.changed_hives=changed;run.finished_at=datetime.utcnow()
            for v in visits:
                if v.status=='screening':v.status=run.prev_statuses.get(str(v.id),'recorded')
            db.commit();return 'voided'
        points={p['visit_id']:p for p in run.points}
        for v in visits:
            p=points[v.id]
            v.anomaly_score=p['score'];v.anomaly_flag=p['flag'];v.decision_margin=p['margin'];v.model_evidence=run.evidence;v.status='screened'
            hive=hives[v.hive_id];hive.care_state='needs_care' if p['flag'] or not v.queen_seen else 'observe'
        run.status='done';run.finished_at=datetime.utcnow();db.commit();return 'done'

def fail(run_id,attempt,error):
    with Session() as db:
        run=db.scalar(select(ScreeningRun).where(ScreeningRun.id==run_id).with_for_update())
        if not run or run.status not in ['pending','running'] or run.attempt!=attempt:return
        run.status='failed';run.error=str(error)[:480];run.finished_at=datetime.utcnow()
        for v in db.scalars(select(Visit).where(Visit.id.in_(run.visit_ids),Visit.status=='screening').order_by(Visit.id).with_for_update()).all():
            v.status=run.prev_statuses.get(str(v.id),'recorded')
        db.commit()

def handle(run_id):
    attempt=claim(run_id)
    if attempt is None:return
    try:
        if not compute(run_id,attempt):return
        outcome=publish(run_id,attempt)
        if outcome=='stale':return
        with Session() as db:run=db.get(ScreeningRun,run_id)
        result={'job_id':run.job_id,'status':run.status,'count':len(run.points or []),'points':run.points,'evidence':run.evidence,'changed_hives':run.changed_hives,'error':run.error}
        Path('/artifacts').mkdir(exist_ok=True);Path(f"/artifacts/screen-{run.job_id}.json").write_text(json.dumps(result))
    except Exception as error:
        traceback.print_exc();fail(run_id,attempt,error)

def main():
    swept=0.0
    while True:
        Path('/tmp/apiary-heartbeat').touch()
        if time.monotonic()-swept>10:
            swept=time.monotonic()
            try:sweep()
            except Exception:traceback.print_exc()
        try:job=queue.blpop('apiary:screening',timeout=3)
        except redis.RedisError:time.sleep(2);continue
        if not job:continue
        try:run_id=json.loads(job[1]).get('run_id')
        except Exception:continue
        if run_id is not None:handle(run_id)

if __name__=='__main__':main()
