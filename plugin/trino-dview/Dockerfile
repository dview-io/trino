FROM trinodb/trino:445
COPY target/trino-dview-435-SNAPSHOT/ /usr/lib/trino/plugin/dview
COPY --chown=root:root ranger-trino-plugin /root/ranger-trino-plugin/
USER root:root
# Install sshfs and sshpass
RUN curl -O https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm && \
    rpm -ivh epel-release-latest-9.noarch.rpm && \
    rm epel-release-latest-9.noarch.rpm
RUN microdnf install -y sshfs sshpass && \
    microdnf clean all
ENV TRINO_MOUNT_CONFIG_PATH /mnt/etc/trino
ENV TRINO_CONFIG_PATH /etc/trino
ENV RANGER_CONFIG_PATH /mnt/ranger
ENV RANGER_ENABLED false
COPY ./replace_env_vars.sh /root/
COPY ./docker-entrypoint.sh /root/
RUN chmod +x /root/docker-entrypoint.sh
RUN chmod +x /root/replace_env_vars.sh

RUN mkdir -p /usr/lib/trino/plugin/hive/lib && \
    curl -L -o /usr/lib/trino/plugin/hive/hdfs/jsch-0.1.55.jar https://repo1.maven.org/maven2/com/jcraft/jsch/0.1.55/jsch-0.1.55.jar
RUN #rm /usr/lib/trino/plugin/hive/trino-hdfs-*.jar
#COPY trino-hdfs-445.jar /usr/lib/trino/plugin/hive/
COPY trino-sftp-435-SNAPSHOT /usr/lib/trino/plugin/sftp
CMD ["/bin/bash", "-c", "/root/docker-entrypoint.sh"]
