# bin/bash
eval $(minikube docker-env)
docker build -t data-service:v1
kubectl run data-service --image=data-service:v1 --port=8086
kubectl expose deployment data-service --type=LoadBalancer
minikube service data-service