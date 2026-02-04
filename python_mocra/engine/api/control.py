from fastapi import APIRouter

router = APIRouter()

@router.post("/stop")
async def stop_engine():
    return {"status": "stopping"}

@router.post("/pause")
async def pause_engine():
    return {"status": "paused"}
