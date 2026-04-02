import subprocess
import yaml
import time

class CephDeployer:
    def __init__(self, config):
        self.nodes = config["nodes"]
        self.user = config["ssh_user"]
        self.mon_ip = config["ceph"]["mon_ip"]
        self.pool = config["ceph"]["pool_name"]
        self.pg = config["ceph"]["pg_num"]

    def run(self, cmd):
        print(f"\n[RUN] {cmd}")
        subprocess.run(cmd, shell=True, check=True)

    def ssh(self, ip, cmd):
        full_cmd = f"ssh -o StrictHostKeyChecking=no {self.user}@{ip} '{cmd}'"
        self.run(full_cmd)

    
    def install_packages(self):
        for name, ip in self.nodes.items():
            self.ssh(ip, """
            apt update -y &&
            apt install -y curl chrony docker.io &&
            systemctl enable --now chrony &&
            systemctl enable --now docker
            """)

    def set_hostnames(self):
        for name, ip in self.nodes.items():
            self.ssh(ip, f"hostnamectl set-hostname {name}")

    def update_hosts(self):
        hosts = "\n".join([f"{ip} {name}" for name, ip in self.nodes.items()])
        for _, ip in self.nodes.items():
            self.ssh(ip, f"echo '{hosts}' >> /etc/hosts")

    def setup_ssh(self):
        self.run("ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_ed25519")

        with open("/root/.ssh/id_ed25519.pub") as f:
            pub = f.read().strip()

        for name, ip in self.nodes.items():
            if name == "node1":
                continue

            self.ssh(ip, f"""
            mkdir -p /root/.ssh &&
            echo '{pub}' >> /root/.ssh/authorized_keys &&
            chmod 700 /root/.ssh &&
            chmod 600 /root/.ssh/authorized_keys
            """)

    def install_cephadm(self):
        self.run("""
        curl --silent --remote-name https://download.ceph.com/rpm-18.2.0/el9/noarch/cephadm &&
        chmod +x cephadm &&
        mv cephadm /usr/local/bin/
        """)

    def bootstrap(self):
        self.run(f"cephadm bootstrap --mon-ip {self.mon_ip}")

    def add_hosts(self):
        for name, ip in self.nodes.items():
            if name != "node1":
                self.run(f"ceph orch host add {name} {ip}")

    def deploy_mons(self):
        nodes = ",".join(self.nodes.keys())
        self.run(f"ceph orch apply mon --placement='{nodes}'")

    def deploy_osd(self):
        self.run("ceph orch device ls --refresh")
        self.run("ceph orch apply osd --all-available-devices")

    def create_pool(self):
        self.run(f"ceph osd pool create {self.pool} {self.pg}")
        self.run(f"rbd pool init {self.pool}")

    
    def run_all(self):
        print("\n🚀 Starting Ceph Deployment...\n")

        self.install_packages()
        self.set_hostnames()
        self.update_hosts()
        self.setup_ssh()
        self.install_cephadm()

        print("\n Bootstrapping...\n")
        self.bootstrap()

        time.sleep(20)

        self.add_hosts()
        self.deploy_mons()

        print("\n⏳ Deploying OSDs...\n")
        time.sleep(20)

        self.deploy_osd()

        time.sleep(30)

        self.create_pool()

        print("\n CLUSTER READY!\n")