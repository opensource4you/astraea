# this dockerfile is generated dynamically
      FROM ghcr.io/skiptests/astraea/spark:3.1.2

      # export ENV
      ENV SPARK_HOME /opt/spark
      WORKDIR /opt/spark
      
