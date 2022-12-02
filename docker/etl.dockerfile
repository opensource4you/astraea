# this dockerfile is generated dynamically
      FROM ghcr.io/skiptests/astraea/spark:3.1.2
      COPY /home/warren/astraea /opt/astraea
      WORKDIR /opt/astraea
      RUN ./gradlew clean shadowJar

      # export ENV
      ENV SPARK_HOME /opt/spark
      WORKDIR /opt/spark
      
