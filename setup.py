#!/usr/bin/env python3

import subprocess
import yaml
import time
import sys


class CephDeployer:

    def __init__(self, config):
        self.nodes = config["nodes"]
        self.user = config["ssh_user"]
        self.mon_ip = config["ceph"]["mon_ip"]
        self.pool = config["ceph"]["pool_name"]
        self.pg = config["ceph"]["pg_num"]
        self.osd_devices = config["osd_devices"]

    # ---------------- UTIL ----------------
    def run(self, cmd, ignore_error=False):
        print(f"\n[RUN] {cmd}")
        result = subprocess.run(cmd, shell=True)
        if result.returncode != 0 and not ignore_error:
            print("❌ Command failed")
            sys.exit(1)

    def ssh(self, ip, cmd):
        self.run(f"ssh -o StrictHostKeyChecking=no {self.user}@{ip} '{cmd}'")

    def log(self, msg):
        print(f"\n{'='*70}\n🚀 {msg}\n{'='*70}")

    def check_ceph(self, stage):
        print(f"\n🔍 STATUS CHECK ({stage})")
        self.run("ceph -s", ignore_error=True)

    def wait_for_ceph(self):
        print("\n⏳ Waiting for Ceph...")
        for _ in range(20):
            res = subprocess.run("ceph -s", shell=True, capture_output=True)
            if b"HEALTH" in res.stdout:
                print("✅ Ceph ready")
                return
            time.sleep(5)
        print("❌ Ceph not ready")
        sys.exit(1)

    # ---------------- STEP 1 ----------------
    def install_packages(self):
        self.log("Installing dependencies (safe)")

        for name, ip in self.nodes.items():
            self.ssh(ip, """
            dpkg -l | grep -q podman || sudo apt install -y podman
            dpkg -l | grep -q chrony || sudo apt install -y chrony
            systemctl is-active chrony || sudo systemctl start chrony
            """)

    # ---------------- STEP 2 ----------------
    def set_hostnames(self):
        self.log("Setting hostnames")

        for name, ip in self.nodes.items():
            self.ssh(ip, f"hostnamectl set-hostname {name}")

    # ---------------- STEP 3 ----------------
    def update_hosts(self):
        self.log("Updating /etc/hosts safely")

        for name, ip in self.nodes.items():
            for n2, ip2 in self.nodes.items():
                self.ssh(ip, f"""
                grep -q '{ip2} {n2}' /etc/hosts || \
                echo '{ip2} {n2}' | sudo tee -a /etc/hosts
                """)

    # ---------------- STEP 4 ----------------
    def install_cephadm(self):
        self.log("Installing cephadm")

        result = subprocess.run("which cephadm", shell=True, capture_output=True)
        if result.returncode == 0:
            print("⚠️ cephadm already installed")
            return

        self.run("""
        curl --silent --remote-name https://download.ceph.com/rpm-18.2.0/el9/noarch/cephadm &&
        chmod +x cephadm &&
        sudo mv cephadm /usr/local/bin/ &&
        sudo cephadm add-repo --release reef &&
        sudo cephadm install
        """)

    # ---------------- STEP 5 ----------------
    def bootstrap(self):
        self.log("Bootstrapping cluster")

        res = subprocess.run("ceph -s", shell=True, capture_output=True)

        if b"cluster:" in res.stdout:
            print("⚠️ Cluster already exists, skipping bootstrap")
            return

        self.run(f"sudo cephadm bootstrap --mon-ip {self.mon_ip} --skip-monitoring-stack")
        self.wait_for_ceph()
        self.check_ceph("After Bootstrap")

    # ---------------- STEP 6 ----------------
    def setup_cephadm_ssh(self):
        self.log("Setting up cephadm SSH")

        self.run("sudo ceph cephadm generate-key || true")
        self.run("sudo ceph cephadm get-pub-key > ceph.pub")

        with open("ceph.pub") as f:
            pub = f.read().strip()

        for name, ip in self.nodes.items():
            if name == "node1":
                continue

            self.ssh(ip, f"""
            sudo mkdir -p /root/.ssh &&
            grep -q "{pub}" /root/.ssh/authorized_keys || \
            echo "{pub}" | sudo tee -a /root/.ssh/authorized_keys &&
            sudo chmod 700 /root/.ssh &&
            sudo chmod 600 /root/.ssh/authorized_keys
            """)

    # ---------------- STEP 7 ----------------
    def add_hosts(self):
        self.log("Adding hosts")

        existing = subprocess.run(
            "ceph orch host ls", shell=True, capture_output=True
        ).stdout.decode()

        for name, ip in self.nodes.items():
            if name in existing:
                print(f"⚠️ {name} already exists")
                continue

            if name != "node1":
                self.run(f"sudo ceph orch host add {name} {ip}")

        self.check_ceph("After adding hosts")

    # ---------------- STEP 8 ----------------
    def deploy_mon_mgr(self):
        self.log("Deploying MON & MGR")

        nodes = ",".join(self.nodes.keys())

        self.run(f"sudo ceph orch apply mon --placement='{nodes}'", ignore_error=True)
        self.run(f"sudo ceph orch apply mgr --placement='{nodes}'", ignore_error=True)

        time.sleep(10)
        self.check_ceph("After MON/MGR")

    # ---------------- STEP 9 ----------------
    def deploy_osd(self):
        self.log("Deploying OSDs safely")

        existing = subprocess.run(
            "ceph osd tree", shell=True, capture_output=True
        ).stdout.decode()

        for node, device in self.osd_devices.items():
            if device in existing:
                print(f"⚠️ OSD exists on {node}:{device}")
                continue

            self.run(f"sudo ceph orch daemon add osd {node}:{device}")

        time.sleep(20)
        self.check_ceph("After OSD")

    # ---------------- STEP 10 ----------------
    def create_pool(self):
        self.log("Creating pool")

        existing = subprocess.run(
            "ceph osd pool ls", shell=True, capture_output=True
        ).stdout.decode()

        if self.pool in existing:
            print("⚠️ Pool already exists")
            return

        self.run(f"sudo ceph osd pool create {self.pool} {self.pg}")
        self.run(f"sudo rbd pool init {self.pool}")

        self.check_ceph("Final")

    # ---------------- MAIN ----------------
    def run_all(self):
        print("\n🚀 STARTING CEPH DEPLOYMENT\n")

        self.install_packages()
        self.set_hostnames()
        self.update_hosts()

        self.install_cephadm()
        self.bootstrap()

        self.setup_cephadm_ssh()
        self.add_hosts()

        self.deploy_mon_mgr()
        self.deploy_osd()

        self.create_pool()

        print("\n🎉 CEPH CLUSTER READY!\n")


# ---------------- ENTRY ----------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    CephDeployer(config).run_all()