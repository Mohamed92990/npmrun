from fastapi import FastAPI

from app.routes.health import router as health_router
from app.routes.query import router as query_router
from app.routes.flags import router as flags_router

app = FastAPI(title="Karbon Timesheets Automation", version="0.1")

app.include_router(health_router)
app.include_router(query_router, prefix="/v1")
app.include_router(flags_router, prefix="/v1")
