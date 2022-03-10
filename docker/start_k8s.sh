
# ===============================[global variables]===============================
declare -r VERSION="1.20.14-00"
declare -r NETCIDR="10.244.0.0/16"
# ===================================[functions]===================================
function showHelp() {
  echo "Usage: [ENV] start_broker.sh [ ARGUMENTS ]"
  echo "Required Argument: "
  echo "    controlplan                              set controlplan or worker" 
  echo " ===========if worker need to set the followers======================"
  echo "    192.168.103.207:6443                     set zookeeper connection" 
  echo "    abcdef.1234567890abcdefm                 set discover token"
  echo "    1234..cdef 1.2.3.4:6443                  set discovery token ca-cert-hash"
  echo "ENV: "
  echo "    VERSION=1.20.14-00                       set host folders used by broker"
}
# ===================================[main]===================================
if [[ "$1" == "help" ]]; then
    showHelp
    exit 0
fi
curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add
sudo apt-add-repository "deb http://apt.kubernetes.io/ kubernetes-xenial main"
sudo apt-get update
sudo apt-get install -y  docker.io
sudo apt-get install -y kubeadm=$VERSION kubelet=&VERSION kubectl=$VERSION
sudo apt-mark hold kubeadm kubelet kubectl
sudo swapoff -a

function controlPlan(){
sudo kubeadm init --pod-network-cidr=$NETCIDR
mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config
sudo kubectl apply -f https://raw.githubusercontent.com/coreos/flannel/master/Documentation/kube-flannel.yml


}


if [[ $1 == "controlplan" ]]; then
controlPlan
exit 0
fi
if [[ $1 == "worker" ]]; then
sudo kubeadm join $2 --discovery-token $3 --discovery-token-ca-cert-hash sha256:$4
exit 0
fi


