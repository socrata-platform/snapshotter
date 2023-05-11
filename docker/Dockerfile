FROM socrata/java8-focal

ENV SERVER_NAME snapshotter

ENV SERVER_ROOT /srv/${SERVER_NAME}/
ENV LOGBACK_CONFIG   ${SERVER_NAME}-logback.xml
ENV SERVER_ARTIFACT  ${SERVER_NAME}-assembly.jar
ENV SERVER_CONFIG ${SERVER_NAME}.conf

# defaults
EXPOSE 6800
ENV JAVA_XMX 512m
ENV JMX_PORT 6801
EXPOSE 6801

WORKDIR $SERVER_ROOT

# Log Settings.
ENV LOG_LEVEL info
ENV LOG_PATTERN %d %-5p [%t]  [%X{X-Socrata-RequestId}] [%X{X-Socrata-Resource}] %c{36} "%X{X-Socrata-Host}" %m%n %ex{full}

# Add resources to the container
COPY ship.d /etc/ship.d
COPY ${SERVER_CONFIG}.j2 $SERVER_ROOT/
COPY $SERVER_ARTIFACT $SERVER_ROOT/
