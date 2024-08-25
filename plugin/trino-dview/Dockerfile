FROM trinodb/trino:444
ENV RANGER_ENABLED false
COPY target/trino-dview-435-SNAPSHOT/ /usr/lib/trino/plugin/dview
COPY --chown=root:root ranger-trino-plugin /root/ranger-trino-plugin/
USER root:root
ENV TRINO_MOUNT_CONFIG_PATH /mnt/etc/trino
ENV TRINO_CONFIG_PATH /etc/trino
ENV RANGER_CONFIG_PATH /mnt/ranger
COPY ./replace_env_vars.sh /root/
COPY ./docker-entrypoint.sh /root/
RUN chmod +x /root/docker-entrypoint.sh
RUN chmod +x /root/replace_env_vars.sh
CMD ["/bin/bash", "-c", "/root/docker-entrypoint.sh"]
