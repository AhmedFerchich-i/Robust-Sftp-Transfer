
import paramiko
import os
from paramiko import SFTPClient
import stat
import time

class sftp_retrieve():
    def establish_connection(self,host, port, username, password):
        ssh_connection = paramiko.SSHClient()
        ssh_connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            print('begin ssh connection')
            ssh_connection.connect(hostname=host,port=port,username=username,password=password)
            print('ssh success')
            sftp_connection=ssh_connection.open_sftp()
            print('sftp connection opened successfully')
            return sftp_connection
        except Exception as e:
            print('error establishing connection ',e)
            return None
        

    def check_if_remote_path_is_dir(self,sftp_connection:SFTPClient,remote_path):
        try:
         print('begin check IF remote path is dir ')
         print('remote path ',remote_path)
         check_if_dir=stat.S_ISDIR(sftp_connection.stat(remote_path).st_mode)
         print(check_if_dir)
         return check_if_dir
        except Exception as e:
         return False
    

    def check_file_existance(self,remote_file,local_path):
        print('begin check file existance')
        path_to_check=os.path.join(local_path,remote_file)
        file_existance=os.path.exists(path_to_check)
        print('file existance ',file_existance)
        return file_existance
    def check_file_transfer_completion(self,remote_path,local_path,sftp_connection:SFTPClient,filename):
        try:

         print('begin check file transfer')
         print('remote path ', remote_path)
         print('local path ', local_path)
         remote_file_size=sftp_connection.stat(remote_path).st_size
         print('remote file size ',remote_file_size)
         local_file_path=os.path.join(local_path,filename)
         local_file_size=os.path.getsize(local_file_path)
         print('local file size ', local_file_size)
         return remote_file_size==local_file_size
        except Exception as e :
            return False
    def check_folder_transfer_completion(self,remote_path,local_path,sftp_connection:SFTPClient):
        print('begin check folder transfer completion')
        print('remote path ', remote_path)
        print('local path ', local_path)
        remote_size=self.get_remote_directory_size(remote_path=remote_path,sftp_connection=sftp_connection)
        print('remote size ', remote_size)
        local_size=self.get_local_directory_size(local_path)
        print('local size ', local_size)
        return (remote_size==local_size)

    def get_local_directory_size(self,local_path):
        print('begin get local directory size')
        list=os.listdir(local_path)
        size=0
        for i in list:
            path=os.path.join(local_path,i)
            if os.path.isfile(path=path):
                size+=os.path.getsize(path)
                
            else:
                size+=self.get_local_directory_size(path)
        return size


    def get_remote_directory_size(self,remote_path,sftp_connection:SFTPClient):
        print('begin get remote directory size')
        remote_list=sftp_connection.listdir(remote_path)
        remote_size=0
        for item in remote_list:
            if not self.check_if_remote_path_is_dir(sftp_connection=sftp_connection,remote_path=remote_path+'/'+item):
             remote_size+=sftp_connection.stat(remote_path+'/'+item).st_size

            else:
                remote_size+=self.get_remote_directory_size(remote_path=remote_path+'/'+item,sftp_connection=sftp_connection)
        return remote_size


    def tranfer_file_first_time(self,remote_path,local_path,sftp_connection:SFTPClient):
        print('begin transfer file first time')
        #time.sleep(30)
        #raise Exception('simulated network failure exception')
        sftp_connection.put(local_path,remote_path)
    def resume_file_transfer_from_breakage(self,remote_path,local_path,filename,sftp_connection:SFTPClient):
        print('begin file transfer from breakage')
        local_file=os.path.join(local_path,filename)
        local_size=os.path.getsize(local_file)
        with sftp_connection.file(remote_path,'rb') as remote_file:
            remote_file.seek(local_size)
            data=remote_file.read(1024)
            with open(local_file,'wb') as l_f:
                while data:
                    l_f.write(data)
                    data=remote_file.read(1024)
            l_f.close()


           


    def transfer_folder(self,local_path,remote_folder:str,sftp_connection:SFTPClient):
        print('begin transfer folder')
        print('remote path at the beginning of transfer folder ',remote_folder)
        print('local path at the beginning of transfer folder',local_path)
        remote_list=sftp_connection.listdir(remote_folder)
        directory_name=remote_folder.split('/')[-1]
        print('directory name ',directory_name)
        new_local_path=os.path.join(local_path,directory_name)
        print('new local path', new_local_path)
        if not os.path.exists(new_local_path):
         os.mkdir(new_local_path)
        self.crawl_and_transfer(remote_list=remote_list,remote_folder=remote_folder,new_local_path=new_local_path,sftp_connection=sftp_connection)
        print('end folder transfer')
        return new_local_path
    

    def crawl_and_transfer(self,remote_list,remote_folder:str,new_local_path:str,sftp_connection:SFTPClient):
        for item in remote_list:
            remote_path=remote_folder+'/'+item
            if self.check_if_remote_path_is_dir(sftp_connection,remote_path):
                new_local_path_for_folder=os.path.join(new_local_path,item)
                print('new local path before recursion',new_local_path_for_folder)
                if not os.path.exists(new_local_path_for_folder):
                 os.mkdir(new_local_path_for_folder)
                #new_remote_path=remote_path+'/'+item
                #print('new remote path ',new_remote_path)
                remote_list=sftp_connection.listdir(remote_path)
                self.crawl_and_transfer(new_local_path=new_local_path_for_folder,remote_folder=remote_path,remote_list=remote_list,sftp_connection=sftp_connection)
            elif not self.check_if_remote_path_is_dir(sftp_connection,remote_path):
                print('new local path in tranfer folder ',new_local_path)
                new_local_path_for_file=os.path.join(new_local_path,item)
                print('new local path for file ',new_local_path_for_file)
                self.transfer_file(item,new_local_path_for_file,remote_path,sftp_connection)
                #comment this later
                #time.sleep(50)
            else:
                sftp_connection.close()
    

    def transfer_file(self,filename,local_path,remote_path,sftp_connection:SFTPClient):
        print('begin transfer file ')
        print('remote file path ', remote_path)
        print('local path for file  ', local_path)
        file_existance_check=self.check_file_existance(filename,local_path)
        if file_existance_check:
            file_completion_ckeck=self.check_file_transfer_completion(remote_path,local_path,sftp_connection,filename)
            if not file_completion_ckeck:
                self.resume_file_transfer_from_breakage(remote_path,local_path,filename,sftp_connection)
            
        else:
            #raise Exception('simulating network failure')
            sftp_connection.get(remotepath=remote_path,localpath=local_path)
        print('end file transfer')


    
 