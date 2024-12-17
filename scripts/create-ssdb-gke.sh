#!/bin/bash

# https://cloud.google.com/kubernetes-engine/docs/tutorials/automatically-bootstrapping-gke-nodes-with-daemonsets

PROJECTID="sig-architecture-poc"
GKE_SERVICE_ACCOUNT_NAME="sa-ssdb-poc-gke"


if [[ -z $PROJECTID ]]; then
    echo " Project ID is missing"
    exit
else 
    echo $PROJECTID
fi


echo -e "\n*********************************************************\n"
echo  "Project ID is..... "$PROJECTID
gcloud config set project ${PROJECTID}
gcloud config set compute/region us-west1
gcloud config set compute/zone us-west1-a
gcloud services enable container.googleapis.com
echo -e "\n*********************************************************\n"
# Create the service account
# echo -e "\ncreating service account: $GKE_SERVICE_ACCOUNT_NAME\n"
# gcloud iam service-accounts create "$GKE_SERVICE_ACCOUNT_NAME" --display-name="$GKE_SERVICE_ACCOUNT_NAME"

# sleep 5

# Get the service account email address
echo -e "\nGetting service account email address\n"
GKE_SERVICE_ACCOUNT_EMAIL="$(gcloud iam service-accounts list --format='value(email)' --filter=displayName:"$GKE_SERVICE_ACCOUNT_NAME")"

echo -e "\nService account email address is: $GKE_SERVICE_ACCOUNT_EMAIL\n"

# Grant roles to the service account
# echo -e "\nGranting roles to service account\n"
# gcloud projects add-iam-policy-binding "$(gcloud config get-value project 2> /dev/null)" --member serviceAccount:"$GKE_SERVICE_ACCOUNT_EMAIL" --role roles/compute.admin
# gcloud projects add-iam-policy-binding "$(gcloud config get-value project 2> /dev/null)" --member serviceAccount:"$GKE_SERVICE_ACCOUNT_EMAIL" --role roles/monitoring.viewer
# gcloud projects add-iam-policy-binding "$(gcloud config get-value project 2> /dev/null)" --member serviceAccount:"$GKE_SERVICE_ACCOUNT_EMAIL" --role roles/monitoring.metricWriter
# gcloud projects add-iam-policy-binding "$(gcloud config get-value project 2> /dev/null)" --member serviceAccount:"$GKE_SERVICE_ACCOUNT_EMAIL" --role roles/logging.logWriter
# gcloud projects add-iam-policy-binding "$(gcloud config get-value project 2> /dev/null)" --member serviceAccount:"$GKE_SERVICE_ACCOUNT_EMAIL" --role roles/iam.serviceAccountUser

# Create the network
# echo -e "Creating network ssdb-poc-network.....................\n"
# gcloud compute networks create ssdb-poc-network --subnet-mode=custom

# Create the subnet
# echo -e "Creating ssdb-poc-subnet.......................................\n"
# gcloud compute networks subnets create ssdb-poc-subnet \
#     --purpose=PRIVATE \
#     --role=ACTIVE \
#     --region=us-west1 \
#     --network=ssdb-poc-network \
#     --range=10.122.0.0/16 \
#     --secondary-range my-pods=10.123.0.0/16,my-services=10.124.0.0/20 \
#     --enable-private-ip-google-access

# Create firewall rules to allow specific traffic
# echo -e "Creating firewall rules to allow tcp:22 80 443 8080 traffic.......\n"
# gcloud compute firewall-rules create ssdb-poc-fw-allow-proxies \
#   --network=ssdb-poc-network \
#   --action=allow \
#   --direction=ingress \
#   --source-ranges=0.0.0.0/0 \
#   --rules=tcp:22,tcp:80,tcp:443,tcp:8080,tcp:3306

# Create the NAT router
# echo -e "\nCreating NAT router......\n"
# gcloud compute routers create ssdb-poc-nat-router \
#     --network ssdb-poc-network \
#     --region us-west1

# Create the NAT configuration
# gcloud compute routers nats create ssdb-poc-nat-config \
#     --router-region us-west1 \
#     --router ssdb-poc-nat-router \
#     --nat-all-subnet-ip-ranges \
#     --auto-allocate-nat-external-ips

# echo -e "\n NAT routers created\n.........."
# gcloud compute routers list


#gcloud container clusters create ssdb-poc-cluster \
#    --cluster-version "1.30.6-gke.1125000" \
#    --release-channel "regular"  \
#    --image-type "UBUNTU_CONTAINERD" \
#    --machine-type "n2-standard-8" \
#    --disk-type "pd-ssd" \
#    --disk-size "256" \
#    --metadata disable-legacy-endpoints=true \
#    --region "us-west1" \
#    --num-nodes "4" \
#    --enable-dns-access \
#    --enable-ip-access \
#    --enable-master-authorized-networks \
#    --master-authorized-networks 136.226.68.202/32,216.208.191.234/32\
#    --network ssdb-poc-network \
#    --subnetwork ssdb-poc-subnet \
#    --cluster-secondary-range-name my-pods \
#    --services-secondary-range-name my-services \
#    --logging=SYSTEM,WORKLOAD \
#    --monitoring=SYSTEM,STORAGE,POD,DEPLOYMENT,STATEFULSET,DAEMONSET,HPA,CADVISOR,KUBELET \
#    --enable-private-nodes \
#    --enable-ip-alias \
#    --no-enable-basic-auth \
#    --no-issue-client-certificate \
#    --node-labels=app=default-init \
#    --node-locations=us-west1-a \
#    --region=us-west1 \
#    --service-account="$GKE_SERVICE_ACCOUNT_EMAIL"



#gcloud beta container --project "sig-architecture-poc" clusters create "cluster-1" --region "us-central1" \
#                      --tier "standard" --no-enable-basic-auth --cluster-version "1.30.6-gke.1125000" \
#                      --release-channel "regular" --machine-type "n2-standard-8" --image-type "UBUNTU_CONTAINERD" \
#                      --disk-type "pd-ssd" --disk-size "100" --metadata disable-legacy-endpoints=true \
#                      --scopes "https://www.googleapis.com/auth/devstorage.read_only","https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring","https://www.googleapis.com/auth/servicecontrol","https://www.googleapis.com/auth/service.management.readonly","https://www.googleapis.com/auth/trace.append" \
#                      --num-nodes "3" --logging=SYSTEM,WORKLOAD --monitoring=SYSTEM,STORAGE,POD,DEPLOYMENT,STATEFULSET,DAEMONSET,HPA,CADVISOR,KUBELET \
#                      --enable-ip-alias --network "projects/sig-architecture-poc/global/networks/fast-dast-poc-net" \
#                      --no-enable-intra-node-visibility --default-max-pods-per-node "110" \
#                      --enable-ip-access --security-posture=standard --workload-vulnerability-scanning=disabled --no-enable-master-authorized-networks --no-enable-google-cloud-access \
#                      --addons HorizontalPodAutoscaling,HttpLoadBalancing,GcePersistentDiskCsiDriver --enable-autoupgrade --enable-autorepair --max-surge-upgrade 1 --max-unavailable-upgrade 0 \
#                      --binauthz-evaluation-mode=DISABLED --enable-managed-prometheus --enable-shielded-nodes

sleep 10

# get container cluster credentials
echo -e "\nGetting container cluster credentials\n"
gcloud container clusters get-credentials ssdb-poc-cluster --region=us-west1


# create namespace
#echo -e "\nCreating namespace\n"
#kubectl create namespace sdb-namespace


# get namespaces
echo -e "\nGetting namespaces\n"
kubectl get namespaces


# apply daemonset
echo -e "\nApplying Daemonset\n"
kubectl apply -f k8s/daemon-set.yaml


# start ssdb cluster deployment
echo -e "\nStarting SSDB cluster deployment\n"

echo -e "\nCreating sdb-rbac.yaml\n"
kubectl create -f  k8s/sdb-rbac.yaml


# create crd
echo -e "\nCreating crd\n"
kubectl create -f k8s/sdb-cluster-crd.yaml

# deploy operator
echo -e "\nDeploying operator\n"
kubectl create -f k8s/sdb-operator.yaml

#get pods
echo -e "\nGetting pods\n"
kubectl get pods 



# create ssdb cluster

# Namespace where the sdb-operator pod is running
NAMESPACE="default"

# Pod name prefix
POD_NAME_PREFIX="sdb-operator"

# Function to check if the sdb-operator pod is running
check_sdb_operator_pod() {
  # Get the status of the sdb-operator pod
  POD_STATUS=$(kubectl get pods -n $NAMESPACE -l name=$POD_NAME_PREFIX -o jsonpath="{.items[0].status.phase}")

  # Check if the pod status is "Running"
  if [ "$POD_STATUS" == "Running" ]; then
    echo "sdb-operator pod is running."
    return 1
  else
    echo "sdb-operator pod is not running. Current status: $POD_STATUS"
    return 0
  fi
}


sleep 10
# check sd-operator pod is running
echo -e "\nChecking if sdb-operator pod is running\n"
check_sdb_operator_pod
if [ $? -eq 1 ]; then
    echo -e "\nCreating ssdb cluster\n"
    kubectl create -f k8s/sdb-cluster.yaml
else
    echo "Error: sdb-operator pod is not running. Cannot create ssdb cluster."
fi

#get pods
echo -e "\nGetting pods\n"
kubectl get pods 