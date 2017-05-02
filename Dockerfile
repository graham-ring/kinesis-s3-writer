FROM openjdk:8

RUN touch /usr/lib/jvm/java-8-openjdk-amd64/release

ENV SCALA_VERSION 2.12.1
ENV SCALA_PATH /scala
RUN mkdir $SCALA_PATH
RUN curl -fsL http://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz | tar xfz - -C /tmp/
RUN cp -r /tmp/scala-$SCALA_VERSION/* $SCALA_PATH/
ENV PATH $PATH:$SCALA_PATH/bin
RUN echo $PATH

ENV SBT_VERSION 0.13.15
RUN curl -L -o sbt-$SBT_VERSION.deb http://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb 
RUN dpkg -i sbt-$SBT_VERSION.deb
RUN rm sbt-$SBT_VERSION.deb
RUN apt-get update
RUN apt-get install sbt
RUN sbt sbtVersion

WORKDIR /root