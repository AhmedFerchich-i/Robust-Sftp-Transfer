from transfer_service import sftp_retrieve
import os
def transfer_task(host:str, port, username, password,remote_path,local_path,max_retry):
    SFTP_Retrieve=sftp_retrieve()
    
    sftp_connection=SFTP_Retrieve.establish_connection(host=str(host),port=int(port),username=username,password=password)
    attempts_count=0

    if sftp_connection:
        directory_check=SFTP_Retrieve.check_if_remote_path_is_dir(sftp_connection=sftp_connection,remote_path=remote_path)
        if directory_check:
            transfer_completion_check=False
            while ((not transfer_completion_check) and attempts_count<=max_retry) :
             try:
                
                print('attempts count ', attempts_count)
                local_dir_path=SFTP_Retrieve.transfer_folder(local_path=local_path,remote_folder=remote_path,sftp_connection=sftp_connection)
                transfer_completion_check=SFTP_Retrieve.check_folder_transfer_completion(remote_path=remote_path,local_path=local_dir_path,sftp_connection=sftp_connection)
                #attempts_count+=1
             except Exception as e :
                print('exception ',e)
                attempts_count+=1
        else:
            file_name=os.path.basename(remote_path)
            transfer_completion_check=False
            while (( not transfer_completion_check ) and attempts_count<=max_retry ):
               try:
                  print('attempts counter for file ', attempts_count)
                  SFTP_Retrieve.transfer_file(local_path=os.path.join(local_path,file_name),remote_path=remote_path,sftp_connection=sftp_connection,filename=file_name)
                  transfer_completion_check=SFTP_Retrieve.check_file_transfer_completion(remote_path=remote_path,local_path=local_path,sftp_connection=sftp_connection,filename=file_name)
               except Exception as e :
                  attempts_count+=1
    sftp_connection.close()