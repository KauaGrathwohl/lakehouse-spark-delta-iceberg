FROM ubuntu:latest

# Atualiza o sistema e instala pacotes necessários
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    python3 \
    python3-pip \
    wget \
    && pip3 install pyspark \
    delta-spark \
    pyspark-iceberg \
    jupyter \
    pandas \
    numpy \
    matplotlib

# Baixa e descompacta o Spark
ENV SPARK_VERSION 3.3.1
RUN wget http://dl.apache.org/spark/$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.3.1.tgz \
    && tar xzf spark-$SPARK_VERSION-bin-hadoop3.3.1.tgz \
    && mv spark-$SPARK_VERSION-bin-hadoop3.3.1 /opt/spark

# Configura as variáveis de ambiente
ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:$PATH

# Exponha a porta do Jupyter Notebook
EXPOSE 8888

# Comando para iniciar o Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]