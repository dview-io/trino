FROM trinodb/trino:433
COPY src/main/resources/dview.properties /etc/trino/catalog/dview.properties
COPY src/main/resources/mysql.properties /etc/trino/catalog/mysql.properties
COPY target/trino-dview-435-SNAPSHOT/ /usr/lib/trino/plugin/dview
COPY --chown=root:root ranger-trino-plugin /root/ranger-trino-plugin/
USER root:root
RUN /root/ranger-trino-plugin/enable-trino-plugin.sh
CMD ["/usr/lib/trino/bin/run-trino"]
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s \
CMD /usr/lib/trino/bin/health-check
