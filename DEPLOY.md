



docker buildx build  --push --tag justinsb/csi-lvm-controller:latest . ; \
 k delete pvc -n default --all; \
 k delete statefulset csi-lvmplugin; k wait --for=delete statefulset csi-lvmplugin; \
 deploy/kubernetes-latest/deploy.sh 
