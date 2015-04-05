from dropbox.rest import ErrorResponse

__author__ = 'gipmon'
import paramiko
import json
import os
import time
import zipfile
import shutil
import dropbox

#Loading the configuration file, it has the access_token, user_id and others configs
config_file = open("config/config.json", "r")
config_file = json.load(config_file)

#load ssh key
mykey = paramiko.RSAKey.from_private_key_file(config_file['config']['rsa_key_path'], config_file['config']['rsa_passphrase'])

#open ssh connection
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.load_system_host_keys()
client.connect(config_file['config']['host'], username=config_file['config']['username'], pkey=mykey)

#open sftp connection
sftp_client = client.open_sftp()


#ssh function to get the output from the server when the command is executed
def ssh(cmd):
    out = []
    msg = [stdin, stdout, stderr] = client.exec_command(cmd)
    for item in msg:
        try:
            for line in item:
                out.append(line.strip('\n'))
        except Exception, e:
            pass

    return list(out)


def zipdir(path, zip):
    for root, dirs, files in os.walk(path):
        for file in files:
            zip.write(os.path.join(root, file))


if not os.path.exists(config_file['config']['tmp_backup_folder']):
    os.makedirs(config_file['config']['tmp_backup_folder'])

for database in config_file['databases']:
    if not os.path.exists(config_file['config']['tmp_backup_folder']+database):
        os.makedirs(config_file['config']['tmp_backup_folder']+database)

    #create backup file
    ssh('mysqldump -u'+config_file['config']['mysql_user']+' -p'+config_file['config']['mysql_pwd']+' '+database+' > tmp.sql')
    #open sftp connection
    #get the bakup file to the current dir
    remote_file = sftp_client.get("tmp.sql", config_file['config']['tmp_backup_folder']+database+"/dump"+time.strftime("%d%m%Y")+".sql")
    #remove the tmp.sql
    ssh('rm tmp.sql')

#close the connection
client.close()

#create zip file of the backups
zipf = zipfile.ZipFile('backup'+time.strftime("%d%m%Y")+'.zip', 'w')
zipdir(config_file['config']['tmp_backup_folder'], zipf)
zipf.close()

#delete the backup files
shutil.rmtree(config_file['config']['tmp_backup_folder'])

#store to dropbox
client_dropbox = dropbox.client.DropboxClient(config_file['dropbox']['auth_token'])
f = open('backup'+time.strftime("%d%m%Y")+'.zip', 'rb')
response = client_dropbox.put_file(config_file['dropbox']['zip_file_prefix']+'backup'+time.strftime("%d%m%Y")+'.zip', f)

#open links.json
links_file = open("config/links.json", "r")
links_file = json.load(links_file)

if len(links_file['links']) > config_file['dropbox']['keep_files_for_days']:
    removed = False

    while not removed and len(links_file['links']) != 0:
        try:
            client_dropbox.file_delete(links_file['links'][0])
            removed = True
        except ErrorResponse:
            pass

        if len(links_file['links']) <= 1:
            break

        links_file['links'] = links_file['links'][1:]

#store the link
links_file['links'] += [response['path']]
j = json.dumps(links_file, indent=4)
f = open("config/links.json", 'w')
print >> f, j
f.close()

#remove zip file
os.remove('backup'+time.strftime("%d%m%Y")+'.zip')