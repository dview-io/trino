FROM trinodb/trino:469
COPY target/trino-dview-469/ /usr/lib/trino/plugin/dview
#COPY target/trino-sftp-469/ /usr/lib/trino/plugin/sftp
COPY target/trino-kafka-469/ /usr/lib/trino/plugin/kafka
#COPY --chown=root:root ranger-trino-plugin /root/ranger-trino-plugin/
COPY rubix-core-0.3.15.jar /usr/lib/trino/plugin/hive/
COPY rubix-hadoop2-0.3.15.jar /usr/lib/trino/plugin/hive/
USER root:root
ENV TRINO_MOUNT_CONFIG_PATH=/mnt/etc/trino
ENV TRINO_CONFIG_PATH=/etc/trino
#ENV RANGER_CONFIG_PATH /mnt/ranger
ENV RANGER_ENABLED false
COPY ./replace_env_vars.sh /root/
COPY ./docker-entrypoint.sh /root/
RUN chmod +x /root/docker-entrypoint.sh
RUN chmod +x /root/replace_env_vars.sh
RUN chmod +x /usr/lib/trino/plugin/hive/rubix-core-0.3.15.jar
RUN chmod +x /usr/lib/trino/plugin/hive/rubix-hadoop2-0.3.15.jar

CMD ["/bin/bash", "-c", "/root/docker-entrypoint.sh"]
