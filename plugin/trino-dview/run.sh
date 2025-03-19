#mvn clean package -DskipTests
#
#if [ $? -ne 0 ]; then
#    echo "mvn build failed"
#    exit 1
#fi


docker stop trino
docker rm trino
docker run --name trino -d -p 8081:8081\
     --volume $PWD/target/trino-dview-469:/usr/lib/trino/plugin/dview\
     --volume $PWD/src/main/resources/dview.properties:/etc/trino/catalog/dview.properties\
     --volume /Users/apple-macbookpro/dviewProjects/fortress/bin/test_data:/Users/shreyasb/worskpace/dview/spring/schema-fortress/bin/test_data\
     -e AWS_PROFILE_NAME=ap\
     -e ENV=local\
     trinodb/trino:469
