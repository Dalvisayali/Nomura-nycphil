from fastapi import APIRouter
from fastapi import FastAPI, HTTPException, Depends, status
from sqlalchemy.orm import Session
from typing import List
from fastapi.responses import JSONResponse

import db_utils.models as models

from db_utils import SessionLocal, db_service

router = APIRouter()

# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/api/v1/concerts_per_season/")
async def concerts_per_season(db: Session = Depends(get_db)):
    try:
        result = db_service.get_concerts_per_season(db)
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(
                status_code=500, detail=f"{str(e)}")
    return JSONResponse(content=result)

@router.get("/api/v1/most_common_conductor/")
async def most_common_conductor(db: Session = Depends(get_db)):
    try:
        result = db_service.get_most_common_conductor(db)
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(
                status_code=500, detail=f"{str(e)}")
    return JSONResponse(content=result)

@router.get("/api/v1/works_per_composer/")
async def works_per_composer(db: Session = Depends(get_db)):
    try:
        result = db_service.get_works_per_composer(db)
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(
                status_code=500, detail=f"{str(e)}")
    return JSONResponse(content=result)

@router.get("/api/v1/soloists_by_instrument/")
async def soloists_by_instrument(db: Session = Depends(get_db)):
    try:
        result = db_service.get_soloists_by_instrument(db)
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(
                status_code=500, detail=f"{str(e)}")
    return JSONResponse(content=result)

@router.get("/api/v1/most_frequent_compositions_by_composer/")
async def most_frequent_compositions_by_composer(composer, db: Session = Depends(get_db)):
    try:
        result = db_service.get_most_frequent_compositions_by_composer(db, composer)
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(
                status_code=500, detail=f"{str(e)}")
    return JSONResponse(content=result)

