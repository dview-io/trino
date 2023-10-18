FROM trinodb/trino:429
COPY src/main/resources/dview.properties /etc/trino/catalog/dview.properties
COPY target/trino-dview-430-SNAPSHOT /usr/lib/trino/plugin/dview
CMD ["/usr/lib/trino/bin/run-trino"]
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s \
  CMD /usr/lib/trino/bin/health-check
