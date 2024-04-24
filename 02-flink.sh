#!/bin/bash
OS=$(uname)
if [ "$OS" = 'Darwin' ]; then
  SED=$(which gsed)
else
  SED=$(which sed)
fi
$SED -i "s/MINIKUBE_IP/$(minikube ip)/g" kafka.yaml
kubectl apply -f kafka.yaml
kubectl apply -f user.yaml
echo "waiting for kafka to start, may take some time to download down images"
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=400s
git checkout kafka.yaml
echo "creating transactions topic, may take some time to download kafka image"
kubectl run client -i --image quay.io/strimzi/kafka:latest-kafka-3.7.0 --restart=Never --rm --pod-running-timeout 2m0s -- bin/kafka-topics.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic transactions --partitions 3 --create
echo "building fraud-detection jar"
mvn clean package
echo "building fraud-detection image in minikube"
minikube image build . -t fraud-detection:latest
echo "applying flink fraud detection deployment"
kubectl apply -f frauddetection_pvc.yaml
kubectl apply -f frauddetection_ha.yaml
