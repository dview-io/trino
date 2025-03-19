#mvn clean package -DskipTests
#
#if [ $? -ne 0 ]; then
#    echo "mvn build failed"
#    exit 1
#fi
#--volume $PWD/ranger-trino-plugin:/root/ranger-trino-plugin/\
#      --volume $PWD/access-control.properties:/etc/trino/access-control.properties\
#      --volume $PWD/ranger-trino-security.xml:/etc/trino/ranger-trino-security.xml\
#      --volume $PWD/ranger-trino-audit.xml:/etc/trino/ranger-trino-audit.xml\
#--volume $PWD/access-control.properties:/usr/lib/trino/plugin/apache-ranger/conf/access-control.properties\
#      --volume $PWD/ranger-trino-security.xml:/usr/lib/trino/plugin/apache-ranger/conf/ranger-trino-security.xml\
#      --volume $PWD/ranger-trino-audit.xml:/usr/lib/trino/plugin/apache-ranger/conf/ranger-trino-audit.xml\
docker stop trino
docker rm trino
docker run --name trino -d -p 8081:8081\
      --volume $PWD/target/trino-kafka-469:/usr/lib/trino/plugin/kafka\
      --volume $PWD/kafka.properties:/etc/trino/catalog/sms.properties\
      --volume $PWD/json:/mnt/data/repo/schemas/repo/schemas/\
      --volume $PWD/access-control.properties:/etc/trino/access-control.properties\
      --volume $PWD/ranger-trino-security.xml:/usr/lib/trino/plugin/apache-ranger/conf/ranger-trino-security.xml\
      --volume $PWD/ranger-trino-audit.xml:/usr/lib/trino/plugin/apache-ranger/conf/ranger-trino-audit.xml\
      trinodb/trino:469
