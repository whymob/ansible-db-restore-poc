#!/usr/bin/python
from ansible.module_utils.basic import AnsibleModule
import subprocess, tempfile, os

def run(module, cmd, input_text=None):
    try:
        rc = subprocess.run(cmd, check=True, capture_output=True, text=True, input=input_text)
        return rc.stdout
    except subprocess.CalledProcessError as e:
        module.fail_json(msg="Command failed", cmd=" ".join(cmd),
                         stderr=e.stderr, stdout=e.stdout, rc=e.returncode)

def docker_exec_rman(module, container, script_text, oracle_sid):
    with tempfile.NamedTemporaryFile('w', delete=False, suffix=".rman") as tf:
        tf.write(script_text)
        host_path = tf.name
    try:
        container_path = f"/tmp/{os.path.basename(host_path)}"
        run(module, ["docker","cp",host_path,f"{container}:{container_path}"])
        cmd = ["docker","exec","-e",f"ORACLE_SID={oracle_sid}",
               container,"bash","-lc",f"rman target / cmdfile {container_path}"]
        return run(module, cmd)
    finally:
        try: os.unlink(host_path)
        except Exception: pass

def rman_full(backup_dir):
    return f"""
RUN {{
  SHUTDOWN IMMEDIATE;
  STARTUP MOUNT;
  CATALOG START WITH '{backup_dir}' NOPROMPT;
  RESTORE DATABASE;
  RECOVER DATABASE;
  ALTER DATABASE OPEN RESETLOGS;
}}
"""

def rman_incremental(backup_dir):
    return f"""
RUN {{
  SHUTDOWN IMMEDIATE;
  STARTUP MOUNT;
  CATALOG START WITH '{backup_dir}' NOPROMPT;
  RESTORE DATABASE;
  RECOVER DATABASE;
  ALTER DATABASE OPEN RESETLOGS;
}}
"""

def rman_pit(backup_dir, pit):
    return f"""
RUN {{
  SHUTDOWN IMMEDIATE;
  STARTUP MOUNT;
  CATALOG START WITH '{backup_dir}' NOPROMPT;
  SET UNTIL TIME "TO_DATE('{pit}','YYYY-MM-DD HH24:MI:SS')";
  RESTORE DATABASE;
  RECOVER DATABASE;
  ALTER DATABASE OPEN RESETLOGS;
}}
"""

def main():
    module = AnsibleModule(
        argument_spec=dict(
            container_name=dict(type='str', required=True),
            oracle_sid=dict(type='str', default='XE'),
            restore_type=dict(type='str', required=True, choices=['full','incremental','pit']),
            backup_path=dict(type='str', required=True),
            point_in_time=dict(type='str', required=False)
        ),
        supports_check_mode=True
    )
    p = module.params
    if module.check_mode:
        module.exit_json(changed=False, msg="check_mode: no changes")

    if p["restore_type"] == "full":
        script = rman_full(p["backup_path"])
    elif p["restore_type"] == "incremental":
        script = rman_incremental(p["backup_path"])
    else:
        pit = p.get("point_in_time")
        if not pit:
            module.fail_json(msg="point_in_time required for restore_type=pit")
        script = rman_pit(p["backup_path"], pit)

    out = docker_exec_rman(module, p["container_name"], script, p["oracle_sid"])
    module.exit_json(changed=True, msg="Oracle RMAN restore completed", rman_output=out)


if __name__ == "__main__":
    main()