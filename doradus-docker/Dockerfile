# Base operating system image
FROM centos

# Install Git library
RUN yum -y install git

# Install Java 8
RUN yum remove -y java-1.7.0-openjdk*
RUN yum install -y curl; yum upgrade -y; yum update -y;  yum clean all

ENV JDK_VERSION 8u11
ENV JDK_BUILD_VERSION b12
RUN curl -LO "http://download.oracle.com/otn-pub/java/jdk/$JDK_VERSION-$JDK_BUILD_VERSION/jdk-$JDK_VERSION-linux-x64.rpm" -H 'Cookie: oraclelicense=accept-securebackup-cookie' && rpm -i jdk-$JDK_VERSION-linux-x64.rpm; rm -f jdk-$JDK_VERSION-linux-x64.rpm; yum clean all
ENV JAVA_HOME /usr/java/default

# Install Maven 3 that uses Java 8
ENV MAVEN_VERSION 3.3.3

RUN curl -fsSL http://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz | tar xzf - -C /usr/share \
  && mv /usr/share/apache-maven-$MAVEN_VERSION /usr/share/maven \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven

# check out Doradus from git repo
RUN rm -rf /opt/doradus
RUN mkdir -p /opt/doradus
RUN cd /opt/doradus
RUN git clone https://github.com/dell-oss/Doradus.git /opt/doradus

# build Doradus
WORKDIR /opt/doradus
RUN cd /opt/doradus
RUN mvn clean install dependency:copy-dependencies -Dgpg.skip=true -Dmaven.javadoc.skip=true


# start scripts
ADD start.sh /usr/local/bin/start.sh
RUN chmod a+x /usr/local/bin/start.sh

# porst used by Doradus/Cassandra
EXPOSE 1123 8080

ENTRYPOINT ["start.sh"]
