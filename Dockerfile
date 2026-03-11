FROM public.ecr.aws/lambda/python:3.11

RUN yum install -y java-17-amazon-corretto-devel

ENV SPARK_HOME="/usr/local/lib/python3.11/site-packages/pyspark"
ENV PATH=$PATH:$SPARK_HOME/bin

WORKDIR /var/task

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# --no-cache-dir to not bloat my docker image with installation cache

COPY src/spark_process.py .

CMD [ "spark_process.handler" ]