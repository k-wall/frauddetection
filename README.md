# frauddetection

Flink play-and-learn experiment project.  I've taken the example from the Flink website and turned into a Flink program
that takes input from a `transactions` topic and sinks to the `alerts` topic.  I want to be able to scale up the task
parallelism so that I can see things like shuffles in action.

# Running

You need Minikube, helm, kubectl.
Checkout this repo. `git clone https://github.com/k-wall/frauddetection && cd frauddetection`

1. Setup minikube and install strimzi, cert-manager, flink operators (warning: destructive operation deletes current minikube)
   ```
   ./01-setup.sh
   ```
2. Deploy Kafka, create `transactions` topic and then deploy fraud detection Flink job
   ```
   ./02-setup.sh
   ```
3. Kafka uses nodeport.
   Hack `/etc/hosts` so that minikube resolves to the `$(minikube ip)`
   ```
   KAFKA=minikube:$(kubectl get service my-cluster-kafka-external-bootstrap -o=jsonpath='{.spec.ports[0].nodePort}{"\n"}')
   ```
4. Start a consumer of the alerts topics. 
   ```
   kafka-console-consumer --bootstrap-server  ${KAFKA} --topic alerts --from-beginning --property print.timestamp=true --property print.offset=true --property print.partition=true
   ```
5. Send some transactions to the `transactions` topic.  Some will trigger the noddy fraud rules.
   ```
   kafka-console-producer --bootstrap-server  ${KAFKA} --topic transactions  --property parse.key=true < transactions.json
   ```

Resolved questions:

Q. Why am I not seeing members in my consumer group???  I'm expecting to see two. `kafka-consumer-groups --bootstrap-server  minikube:32484 --group mygroup1 --describe --members`
A. It is the way the Kafka Flink Source is implemented.  https://stackoverflow.com/questions/62718445/current-offset-and-lag-of-kafka-consumer-group-that-has-no-active-members
Q. Why's all the work being done by all than one task.
A. I had too few transactions.

