#!/bin/bash

# Configuration
CLUSTER_NAME="kind" # Change if your cluster has a different name
MASTER_IMAGE="flowgrid-master:latest"
WORKER_IMAGE="flowgrid-worker:latest"

echo "🚀 Building Flowgrid Images..."
docker build -t $MASTER_IMAGE .
docker build -t $WORKER_IMAGE .

echo "📦 Loading images into KIND cluster: $CLUSTER_NAME..."
kind load docker-image $MASTER_IMAGE --name $CLUSTER_NAME
kind load docker-image $WORKER_IMAGE --name $CLUSTER_NAME

echo "🔄 Restarting deployments..."
kubectl rollout restart deployment flowgrid-master
kubectl rollout restart deployment flowgrid-worker
kubectl rollout restart deployment flowgrid-worker-cpu

echo "✅ Done! Check status with: kubectl get pods"
