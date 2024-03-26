# frauddetection

Flink play-and-learn experiment project.  I've taken the example from the Flink website and turned into a Flink program
that takes input from a `transactions` topic and sinks to the `alerts` topic.  I want to be able to scale up the task
parallelism so that I can see things like shuffles in action.

# Running

You need Minikube.

```
MINIKUBE_CPUS=4
MINIKUBE_MEMORY=16384
MINIKUBE_DISK_SIZE=25GB
```

Checkout this repo.


1. Apply the Strimzi QuickStart
   ```
   kubectl create -f 'https://strimzi.io/install/latest?namespace=default' -n default
   ```
2. Create a Kafka
   ```
   oc apply -f kafka.yaml
   ```
3. Kafka uses nodeport.
   Hack /etc/hosts so that minikube resolves to the $(minikube ip)
   KAFKA=minikube:$(kubectl get service my-cluster-kafka-external-bootstrap -o=jsonpath='{.spec.ports[0].nodePort}{"\n"}')
4. Install the Apache Flink Operator using helm.
   ```
   helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
   helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
   ```
5. Create the Flink Deployment
   ```
   kubectl apply -f frauddetection_ha.yaml
   ```
6. Start a consumer of the alerts topics. 
   ```
   kafka-console-consumer --bootstrap-server  ${KAFKA} --topic alerts --from-beginning --property print.timestamp=true --property print.offset=true --property print.partition=true
   ```
7. Send some transactions to the `transactions` topic.  Some will trigger the noddy fraud rules.
   ```
   kafka-console-producer --bootstrap-server  ${KAFKA} --topic transactions  --property parse.key=true < transactions.json
   ```

Open questions:
1. Why am I not seeing members in my consumer group???  I'm expecting to see two. `kafka-consumer-groups --bootstrap-server  minikube:32484 --group mygroup1 --describe --members`
2. Why's all the work being done by all than one task.

