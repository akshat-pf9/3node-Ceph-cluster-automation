# Prerequisites
-   1. OS Requirements 
    - Ubuntu 20.04 / 22.04 (recommended)
    - Root access available
-   2. Network Requirements
    - All nodes must be on the same network
    - Must SSH into all nodes WITHOUT password
    - Root password or sudo access
-  Disk Requirements
    - OS disk → vda
    - Ceph disk → vdb 

# How to run
# On Node 1
git clone <repo link>
cd Ceph_automation
pip install pyyaml
python3 setup.py --config config.yaml