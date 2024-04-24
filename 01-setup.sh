#!/bin/bash
MINIKUBE_CPUS=4
MINIKUBE_MEMORY=16384
MINIKUBE_DISK_SIZE=25GB
minikube delete
minikube start
kubectl create -f 'https://strimzi.io/install/latest?namespace=default' -n default
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.4/cert-manager.crds.yaml
helm install cert-manager jetstack/cert-manager --namespace default --version v1.14.4
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
