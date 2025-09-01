#!/usr/bin/python
from ansible.module_utils.basic import AnsibleModule
import subprocess

def run(module, cmd):
    try:
        rc = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return rc.stdout
    except subprocess.CalledProcessError as e:
        module.fail_json(msg="Command failed", cmd=" ".join(cmd),
                         stderr=e.stderr, stdout=e.stdout, rc=e.returncode)

def sqlcmd(module, container, sa_password, tsql):
    cmd = ["docker","exec",container,
           "/opt/mssql-tools/bin/sqlcmd","-S","localhost",
           "-U","SA","-P",sa_password,"-b","-Q",tsql]
    return run(module, cmd)

def main():
    module = AnsibleModule(
        argument_spec=dict(
            container_name=dict(type='str', required=True),
            sa_password=dict(type='str', required=True, no_log=True),
            db_name=dict(type='str', required=True),
            action=dict(type='str', required=True, choices=['create_db','backup_full','backup_diff','backup_log']),
            backup_path=dict(type='str', default='/backups'),
            file_name=dict(type='str', required=False),
            init=dict(type='bool', default=True)
        ),
        supports_check_mode=True
    )
    p = module.params
    if module.check_mode:
        module.exit_json(changed=False, msg="check_mode: no changes")

    changed = True
    if p["action"] == "create_db":
        tsql = f"""
IF DB_ID(N'{p['db_name']}') IS NULL
BEGIN
  CREATE DATABASE [{p['db_name']}];
END;
ALTER DATABASE [{p['db_name']}] SET RECOVERY FULL;
"""
        sqlcmd(module, p["container_name"], p["sa_password"], tsql)
    else:
        if not p.get("file_name"):
            module.fail_json(msg="file_name is required for backup actions")
        init = "INIT, FORMAT," if p["init"] else ""
        if p["action"] == "backup_full":
            tsql = f"BACKUP DATABASE [{p['db_name']}] TO DISK=N'{p['backup_path']}/{p['file_name']}' WITH {init} STATS=5;"
        elif p["action"] == "backup_diff":
            tsql = f"BACKUP DATABASE [{p['db_name']}] TO DISK=N'{p['backup_path']}/{p['file_name']}' WITH DIFFERENTIAL, {init} STATS=5;"
        else:
            tsql = f"BACKUP LOG [{p['db_name']}] TO DISK=N'{p['backup_path']}/{p['file_name']}' WITH {init} STATS=5;"
        sqlcmd(module, p["container_name"], p["sa_password"], tsql)

    module.exit_json(changed=changed, msg="SQL Server action completed")


if __name__ == "__main__":
    main()