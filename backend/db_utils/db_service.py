from sqlalchemy.orm import Session
import db_utils.models as models
from sqlalchemy.sql import func
from sqlalchemy.orm import class_mapper

def to_dict(model):
    """Convert SQLAlchemy model instance to dictionary."""
    if model is None:
        return None
    return {c.key: getattr(model, c.key) for c in class_mapper(model.__class__).columns}



def get_concerts_per_season(db: Session):
    result = db.query(
        models.Nycphil.season, func.count(models.Concert.concert_id).label('concert_count')
        ).join(models.Concert
        ).group_by(models.Nycphil.season).all()
    return [{'season': r[0], 'concert_count': r[1]} for r in result]

def get_most_common_conductor(db: Session):
    result = db.query(
        models.Work.work_conductor_name, func.count(models.Work.work_id).label('count')
        ).filter(
            models.Work.work_conductor_name != 'Unknown'
        ).group_by(
            models.Work.work_conductor_name
        ).order_by(
            func.count(models.Work.work_id).desc()
        ).first()
    return {'most_common_conductor': result[0], 'count': result[1]}

def get_works_per_composer(db: Session):
    result = db.query(
        models.Work.work_composer_name, func.count(models.Work.work_id).label('work_count')
        ).group_by(
            models.Work.work_composer_name
        ).all()
    return [{'composer': r[0], 'work_count': r[1]} for r in result]

def get_soloists_by_instrument(db: Session):
    result = db.query(
        models.Soloist.soloist_instrument,
        func.count(models.Soloist.soloist_id).label('soloist_count')
        ).group_by(
            models.Soloist.soloist_instrument
        ).all()
    
    return [{'instrument': r[0], 'soloist_count': r[1]} for r in result]

def get_most_frequent_compositions_by_composer(db: Session, composer: str):
    result = db.query(
        models.Work.work_work_title,
        func.count(models.Work.work_id).label('performance_count')
    ).filter(
        models.Work.work_composer_name == composer
    ).group_by(
        models.Work.work_work_title
    ).order_by(
        func.count(models.Work.work_id).desc()
    ).limit(10).all()
    
    return [{'work_title': r[0], 'performance_count': r[1]} for r in result]

