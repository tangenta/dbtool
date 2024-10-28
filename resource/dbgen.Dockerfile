# Build docker image for minikube:
# 1. eval $(minikube docker-env)
# 2. cd project root directory
# 3. docker build -t dbgen:0.0.3 -f resource/dbgen.Dockerfile .
FROM rockylinux:9

COPY ./bin/dbgen /dbgen
COPY ./resource/template.sql /template.sql

WORKDIR /
EXPOSE 9000
ENTRYPOINT ["/dbgen"]
