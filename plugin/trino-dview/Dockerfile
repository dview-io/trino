FROM trinodb/trino:433
#COPY src/main/resources/dview-backup.properties /etc/trino/catalog/dview-backup.properties
COPY src/main/resources/dview-backup.properties.properties /etc/trino/catalog/dview.properties
COPY src/main/resources/mysql.properties /etc/trino/catalog/mysql.properties
#COPY src/main/resources/glue.properties /etc/trino/catalog/glue.properties
COPY src/main/resources/hive.properties /etc/trino/catalog/hive.properties
#COPY target/trino-dview-435-SNAPSHOT/ /usr/lib/trino/plugin/dview
#COPY --chown=root:root ranger-trino-plugin /root/ranger-trino-plugin/
USER root:root
COPY ./replace_env_vars.sh /root/
COPY ./docker-entrypoint.sh /root/
RUN chmod 777 /root/docker-entrypoint.sh
CMD ["/bin/bash", "-c", "/root/docker-entrypoint.sh"]
