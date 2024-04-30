#mvn clean package -DskipTests
#
#if [ $? -ne 0 ]; then
#    echo "mvn build failed"
#    exit 1
#fi

docker stop trino
docker rm trino
docker run --name trino -d -p 8080:8080\
      --volume $PWD/target/trino-dview-435-SNAPSHOT:/usr/lib/trino/plugin/dview\
      --volume $PWD/src/main/resources/glue.properties:/etc/trino/catalog/glue.properties\
      --volume $PWD/src/main/resources/dview-backup.properties:/etc/trino/catalog/dview.properties\
      --volume /Users/shreyasb/worskpace/dview/spring/schema-fortress/bin/test_data:/Users/shreyasb/worskpace/dview/spring/schema-fortress/bin/test_data\
      -e AWS_ACCESS_KEY=AKIAWL4FAPTQODZD53Q6\
      -e accessKey=AKIAWL4FAPTQODZD53Q6\
      -e secretKey=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\
      -e AWS_SECRET_KEY=bDkNakvHcJG5uLdFbQd5fQoKCHIXG3tUXJrhWRgH\
      trinodb/trino:435
