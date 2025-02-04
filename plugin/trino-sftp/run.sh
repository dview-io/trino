mvn clean -DskipTests package

if [ $? -ne 0 ]; then
    echo "mvn build failed"
    exit 1
fi

docker stop trino
docker rm trino
docker run --name trino -d -p 8080:8080\
      --volume $PWD/target/trino-sftp-435-SNAPSHOT:/usr/lib/trino/plugin/sftp\
      --volume $PWD/src/main/resources/sftp.properties:/etc/trino/catalog/sftp.properties\
      --volume $PWD/src/main/resources/log.properties:/etc/trino/log.properties\
      -e TRINO_LOGGING_PROPERTIES=/etc/trino/log.properties\
      trinodb/trino:435
