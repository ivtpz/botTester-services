# bin/bash
eval $(minikube docker-env)
docker build -t data-service:v1 .
kubectl set image deployment/data-service data-service=data-service:v1
minikube service data-service