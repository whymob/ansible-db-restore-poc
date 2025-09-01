#!/usr/bin/python
from ansible.module_utils.basic import AnsibleModule
import subprocess

def run(module, cmd, input_text=None):
    try:
        rc = subprocess.run(cmd, check=True, capture_output=True, text=True, input=input_text)
        return rc.stdout
    except subprocess.CalledProcessError as e:
        module.fail_json(msg="Command failed", cmd=" ".join(cmd),
                         stderr=e.stderr, stdout=e.stdout, rc=e.returncode)

def sqlplus_sysdba(module, container, sql):
    cmd = ["docker","exec","-i",container,"bash","-lc","sqlplus -s / as sysdba"]
    return run(module, cmd, input_text=sql)

def rman(module, container, block):
    cmd = ["docker","exec","-i",container,"bash","-lc","rman target /"]
    return run(module, cmd, input_text=block)

def main():
    module = AnsibleModule(
        argument_spec=dict(
            container_name=dict(type='str', required=True),
            action=dict(type='str', required=True, choices=['enable_archivelog','backup_level0','backup_level1','backup_archivelog','create_sample']),
            backup_path=dict(type='str', default='/backups'),
        ),
        supports_check_mode=True
    )
    p = module.params
    if module.check_mode:
        module.exit_json(changed=False, msg="check_mode: no changes")

    if p["action"] == "enable_archivelog":
        sql = """
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
"""
        sqlplus_sysdba(module, p["container_name"], sql)

    elif p["action"] == "create_sample":
        sql = """
WHENEVER SQLERROR EXIT SQL.SQLCODE
CREATE USER app IDENTIFIED BY app DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO app;
CONN app/app
CREATE TABLE t (id NUMBER PRIMARY KEY, val VARCHAR2(100));
INSERT INTO t VALUES (1, 'initial');
COMMIT;
"""
        sqlplus_sysdba(module, p["container_name"], sql)

    elif p["action"] == "backup_level0":
        rman(module, p["container_name"], f"RUN {{ BACKUP INCREMENTAL LEVEL 0 DATABASE FORMAT '{p['backup_path']}/L0_%U.bkp'; }}")

    elif p["action"] == "backup_level1":
        rman(module, p["container_name"], f"RUN {{ BACKUP INCREMENTAL LEVEL 1 DATABASE FORMAT '{p['backup_path']}/L1_%U.bkp'; }}")

    elif p["action"] == "backup_archivelog":
        rman(module, p["container_name"], f"RUN {{ SQL 'ALTER SYSTEM ARCHIVE LOG CURRENT'; BACKUP ARCHIVELOG ALL FORMAT '{p['backup_path']}/ARC_%U.bkp' NOT BACKED UP 1 TIMES; }}")

    module.exit_json(changed=True, msg="Oracle action completed")


if __name__ == "__main__":
    main()