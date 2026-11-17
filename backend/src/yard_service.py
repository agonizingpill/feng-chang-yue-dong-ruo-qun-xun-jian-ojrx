import uuid,json,logging
from datetime import datetime,timedelta
from sqlalchemy import select,func
from fastapi import HTTPException
from .models import Hive,Visit,CareAction,Audit,ScreeningRun

RUN_STALE=timedelta(seconds=60)

def require(db,model,key,lock=False):
    q=select(model).where(model.id==key)
    if lock:q=q.with_for_update().execution_options(populate_existing=True)
    row=db.scalar(q)
    if not row:raise HTTPException(404,'蜂场记录不存在')
    return row

def audit(db,user,text):db.add(Audit(actor=user.email,action=text))
def latest(db,hive_id):return db.scalar(select(func.max(Visit.id)).where(Visit.hive_id==hive_id))

def save_hive(db,body,user,key=None):
    hive=require(db,Hive,key,True) if key else Hive()
    if key:
        from .season_models import ApiaryMove
        if db.scalar(select(ApiaryMove.id).where(ApiaryMove.hive_id==hive.id,ApiaryMove.state.in_(['requested','approved']))):raise HTTPException(409,'在途转场期间蜂箱档案冻结，可先撤销申请')
    if key and hive.care_state in ['caring','needs_requeen']:raise HTTPException(409,'该蜂群有进行中的处置，王况和框数请在复查簿记录')
    other=db.scalar(select(Hive).where(Hive.code==body.code,Hive.id!=key)) if key else db.scalar(select(Hive).where(Hive.code==body.code))
    if other:raise HTTPException(409,'蜂箱编号已存在')
    for k,v in body.model_dump().items():setattr(hive,k,v)
    db.add(hive);audit(db,user,f'维护蜂箱 {body.code}');db.commit();return hive

def record_visit(db,body,user):
    hive=require(db,Hive,body.hive_id,True)
    if hive.care_state in ['caring','needs_requeen']:raise HTTPException(409,'该箱处置未结束，请在处置簿登记复查，不能覆盖原巡检')
    if db.scalar(select(Visit).where(Visit.code==body.code)):raise HTTPException(409,'巡检编号已存在')
    visit=Visit(**body.model_dump());hive.queen_present=body.queen_seen;hive.colony_frames=body.colony_frames;hive.care_state='observe' if body.queen_seen else 'needs_care'
    db.add(visit);audit(db,user,f'巡箱 {hive.code} / {body.code} {body.weight_kg}kg');db.commit();return visit

def push_run(queue,run_id):
    try:queue.rpush('apiary:screening',json.dumps({'run_id':run_id}))
    except Exception:logging.getLogger(__name__).warning('筛查批次 %s 暂未入队，等待恢复扫描补投',run_id)

def run_payload(run):
    return {'job_id':run.job_id,'status':run.status,'attempt':run.attempt,'actor':run.actor,'total':len(run.visit_ids),'count':len(run.points or []),'points':run.points,'evidence':run.evidence,'changed_hives':run.changed_hives,'error':run.error,'created_at':run.created_at,'updated_at':run.updated_at,'finished_at':run.finished_at}

def enqueue_screen(db,user,queue,key=None):
    if key:
        v=require(db,Visit,key,True)
        if latest(db,v.hive_id)!=v.id:raise HTTPException(409,'该箱已有更新巡检，请筛查最新记录')
        candidates=[v]
    else:
        ids=select(func.max(Visit.id)).group_by(Visit.hive_id)
        candidates=db.scalars(select(Visit).where(Visit.id.in_(ids),Visit.status.in_(['recorded','screened','blocked'])).order_by(Visit.id).with_for_update()).all()
    if not candidates:
        if db.scalar(select(Visit.id).where(Visit.status=='screening').limit(1)):raise HTTPException(409,'上一批筛查尚未发布，请等待完成或从原次重试')
        raise HTTPException(409,'没有可筛查的最新巡检，请先登记读数')
    prev={}
    for v in candidates:
        if v.status not in ['recorded','screened','blocked']:raise HTTPException(409,'已进入处置的巡检不能重新筛查或覆盖原值')
        prev[str(v.id)]=v.status;v.status='screening'
    run=ScreeningRun(job_id=uuid.uuid4().hex,actor=user.email,visit_ids=[v.id for v in candidates],prev_statuses=prev)
    db.add(run);audit(db,user,f'弱群筛查 {len(candidates)} 箱最新巡检');db.commit()
    push_run(queue,run.id);return {'job_id':run.job_id,'count':len(candidates)}

def retry_screen(db,user,queue,job_id):
    run=db.scalar(select(ScreeningRun).where(ScreeningRun.job_id==job_id).with_for_update())
    if not run:raise HTTPException(404,'筛查批次不存在')
    if run.status=='done':raise HTTPException(409,'本次筛查已完成发布，不能重复发布')
    if run.status in ['pending','running'] and run.updated_at and datetime.utcnow()-run.updated_at<RUN_STALE:raise HTTPException(409,'本次筛查仍在等待或执行中，请稍候再试')
    if run.status in ['failed','voided']:
        visits=db.scalars(select(Visit).where(Visit.id.in_(run.visit_ids)).order_by(Visit.id).with_for_update()).all()
        for v in visits:
            if v.status not in ['recorded','screened','blocked']:
                hive=db.get(Hive,v.hive_id);raise HTTPException(409,f"蜂箱 {hive.code if hive else v.hive_id} 的巡检已进入新流程，不能按原次重试")
        prev=dict(run.prev_statuses)
        for v in visits:prev[str(v.id)]=v.status;v.status='screening'
        run.prev_statuses=prev
    run.status='pending';run.error=None;run.changed_hives=None;run.finished_at=None
    audit(db,user,f'从原次重试筛查批次 {run.job_id[:8]}');db.commit()
    push_run(queue,run.id);return run_payload(run)
