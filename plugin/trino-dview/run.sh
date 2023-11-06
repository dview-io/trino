mvn clean package -DskipTests

if [ $? -ne 0 ]; then
    echo "mvn build failed"
    exit 1
fi

docker stop trino
docker rm trino
docker run --name trino -d -p 8080:8080\
      --volume $PWD/target/trino-dview-430-SNAPSHOT:/usr/lib/trino/plugin/dview\
      --volume $PWD/src/main/resources/dview.properties:/etc/trino/catalog/dview.properties\
      --volume /Users/shreyasb/worskpace/dview/spring/schema-fortress/bin/test_data:/Users/shreyasb/worskpace/dview/spring/schema-fortress/bin/test_data\
      -e accessKey=${accessKey}\
      -e secretKey=${secretKey}\
      trino-429-dview
