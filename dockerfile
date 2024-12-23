FROM python:3.11-slim-bullseye

WORKDIR /tasty-trails

RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN wget https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
    tar -xvzf spark-3.4.0-bin-hadoop3.tgz -C /opt && \
    rm spark-3.4.0-bin-hadoop3.tgz && \
    ln -s /opt/spark-3.4.0-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Copy the app folder and requirements.txt into the container
COPY app /tasty-trails/app
COPY requirements.txt /tasty-trails/requirements.txt

# Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r /tasty-trails/requirements.txt

EXPOSE 8000

CMD ["python3", "-m", "streamlit", "run", "/tasty-trails/app/main.py", "--server.port=8000", "--server.address=0.0.0.0"]
