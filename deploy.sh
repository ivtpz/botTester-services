# bin/bash
eval $(minikube docker-env)
sha=$(git log -n 1 --pretty=format:"%H")
tag=${sha:0:6} 
time=$(date +"%H-%M-%S") # So we can deploy with out new commits
docker build -t data-service:sha-$tag-time-$time .
kubectl set image deployment/data-service data-service=data-service:sha-$tag-time-$time
sleep 3
minikube service data-service