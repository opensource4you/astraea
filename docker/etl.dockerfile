# this dockerfile is generated dynamically
      FROM ghcr.io/skiptests/astraea/spark:3.3.1

      # export ENV
      ENV SPARK_HOME /opt/spark
      WORKDIR /opt/spark
      
