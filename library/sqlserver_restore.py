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
    cmd = [
        "docker","exec",container,
        "/opt/mssql-tools/bin/sqlcmd","-S","localhost","-U","SA",
        "-P",sa_password,"-b","-Q",tsql
    ]
    return run(module, cmd)


def restore_full(module, p):
    db = p["db_name"]
    full = p["full_backup"]
    replace = "REPLACE" if p.get("replace", True) else ""
    pre = f"IF DB_ID(N'{db}') IS NOT NULL ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;"
    sqlcmd(module, p["container_name"], p["sa_password"], pre)
    move_clause = ""
    if p.get("move_mappings"):
        move_parts = [
            f"MOVE N'{m['logical_name']}' TO N'{m['physical_name']}'"
            for m in p["move_mappings"]
        ]
        move_clause = ", " + ", ".join(move_parts)
    sqlcmd(module, p["container_name"], p["sa_password"],
           f"RESTORE DATABASE [{db}] FROM DISK=N'{full}' WITH {replace}{move_clause}, STATS=5;")
    sqlcmd(module, p["container_name"], p["sa_password"],
           f"ALTER DATABASE [{db}] SET MULTI_USER;")


def restore_diff(module, p):
    db = p["db_name"]
    full = p["full_backup"]
    diff = p["diff_backup"]
    pre = f"IF DB_ID(N'{db}') IS NOT NULL ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;"
    sqlcmd(module, p["container_name"], p["sa_password"], pre)
    move_clause = ""
    if p.get("move_mappings"):
        move_parts = [
            f"MOVE N'{m['logical_name']}' TO N'{m['physical_name']}'"
            for m in p["move_mappings"]
        ]
        move_clause = ", " + ", ".join(move_parts)
    sqlcmd(module, p["container_name"], p["sa_password"],
           f"RESTORE DATABASE [{db}] FROM DISK=N'{full}' WITH NORECOVERY, REPLACE{move_clause}, STATS=5;")
    sqlcmd(module, p["container_name"], p["sa_password"],
           f"RESTORE DATABASE [{db}] FROM DISK=N'{diff}' WITH RECOVERY, STATS=5;")
    sqlcmd(module, p["container_name"], p["sa_password"],
           f"ALTER DATABASE [{db}] SET MULTI_USER;")


def restore_pit(module, p):
    db = p["db_name"]
    full = p["full_backup"]
    logs = p["log_backups"]
    stopat = p["point_in_time"]
    pre = f"IF DB_ID(N'{db}') IS NOT NULL ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;"
    sqlcmd(module, p["container_name"], p["sa_password"], pre)
    move_clause = ""
    if p.get("move_mappings"):
        move_parts = [
            f"MOVE N'{m['logical_name']}' TO N'{m['physical_name']}'"
            for m in p["move_mappings"]
        ]
        move_clause = ", " + ", ".join(move_parts)
    sqlcmd(module, p["container_name"], p["sa_password"],
           f"RESTORE DATABASE [{db}] FROM DISK=N'{full}' WITH NORECOVERY, REPLACE{move_clause}, STATS=5;")
    if not logs:
        module.fail_json(msg="PIT requires log_backups[] covering the STOPAT time")
    for log in logs[:-1]:
        sqlcmd(module, p["container_name"], p["sa_password"],
               f"RESTORE LOG [{db}] FROM DISK=N'{log}' WITH NORECOVERY, STATS=5;")
    sqlcmd(module, p["container_name"], p["sa_password"],
           f"RESTORE LOG [{db}] FROM DISK=N'{logs[-1]}' WITH STOPAT = '{stopat}', RECOVERY, STATS=5;")
    sqlcmd(module, p["container_name"], p["sa_password"],
           f"ALTER DATABASE [{db}] SET MULTI_USER;")


def main():
    module = AnsibleModule(
        argument_spec=dict(
            container_name=dict(type='str', required=True),
            sa_password=dict(type='str', required=True, no_log=True),
            db_name=dict(type='str', required=True),
            restore_type=dict(type='str', required=True, choices=['full', 'diff', 'pit']),
            full_backup=dict(type='str'),
            diff_backup=dict(type='str'),
            log_backups=dict(type='list', elements='str', default=[]),
            point_in_time=dict(type='str'),
            move_mappings=dict(type='list', elements='dict', default=[]),
            replace=dict(type='bool', default=True),
        ),
        supports_check_mode=True
    )
    p = module.params
    if module.check_mode:
        module.exit_json(changed=False, msg="check_mode: no changes")

    try:
        if p["restore_type"] == "full":
            if not p.get("full_backup"):
                module.fail_json(msg="full_backup is required for full")
            restore_full(module, p)
        elif p["restore_type"] == "diff":
            if not p.get("full_backup") or not p.get("diff_backup"):
                module.fail_json(msg="full_backup and diff_backup required for diff")
            restore_diff(module, p)
        else:
            if not p.get("full_backup") or not p.get("point_in_time") or not p.get("log_backups"):
                module.fail_json(msg="full_backup, point_in_time, log_backups required for pit")
            restore_pit(module, p)
        module.exit_json(changed=True, msg="SQL Server restore completed")
    except Exception as e:
        module.fail_json(msg=str(e))


if __name__ == "__main__":
    main()