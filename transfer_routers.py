from fastapi import APIRouter
from fastapi import BackgroundTasks

from transfer_tasks import transfer_task

sftp_retrieve_router=APIRouter()

@sftp_retrieve_router.get("/")
async def sftp_transfer(host:str, port:int, username:str, password:str, local_path:str, remote_path:str,background_tasks:BackgroundTasks,max_retry:int = 3):
    background_tasks.add_task(transfer_task, host, port, username, password, remote_path, local_path,max_retry)

