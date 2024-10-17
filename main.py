from fastapi import FastAPI,BackgroundTasks

from transfer_tasks import transfer_task
from transfer_routers import sftp_retrieve_router
app=FastAPI()

app.include_router(sftp_retrieve_router)
    





