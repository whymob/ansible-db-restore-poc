# Ansible DB Restore PoC — SQL Server 2019 & Oracle 21c on Docker

## Overview
This PoC:
- Deploys SQL Server 2019 and Oracle 21c XE in Docker
- Restores databases in these scenarios:
  1. Full backup only
  2. Full + Differential (SQL Server) / Level 0 + Level 1 Incremental (Oracle)
  3. Point-in-Time restore for SQL Server (`STOPAT`)
  4. Point-in-Time restore for Oracle via RMAN (`SET UNTIL TIME`)
- All restore logic is implemented in custom Ansible Python modules.

## Prerequisites
- Docker (>= 20.x)
- Python (>= 3.9)
- Ansible (>= 2.15)
- ~6GB RAM allocated to Docker

## Quick Start
```bash
pip install -r requirements.txt
ansible-galaxy collection install -r requirements.yml

# Deploy containers
ansible-playbook playbooks/01_deploy.yml

# Run restore scenarios
ansible-playbook playbooks/10_restore_full.yml
ansible-playbook playbooks/11_restore_diff.yml
ansible-playbook playbooks/12_restore_pit_sql.yml
ansible-playbook playbooks/13_restore_pit_oracle.yml
