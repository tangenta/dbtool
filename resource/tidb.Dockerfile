# Build tidb image for minikube:
# 1. eval $(minikube docker-env)
# 2. cd project root directory
# 3. docker build -t tidb-test:0.0.1 -f resource/tidb.Dockerfile .
FROM rockylinux:9-minimal

COPY ./tidb-server /tidb-server

WORKDIR /
EXPOSE 4000
ENTRYPOINT ["/tidb-server"]
