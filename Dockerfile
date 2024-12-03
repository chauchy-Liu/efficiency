FROM python:3.9

RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

COPY requirements.txt /src/

#RUN mv /etc/apt/source.list /etc/apt/source.list.bak
# pip3 install torch==2.2.2+cpu -f https://download.pytorch.org/whl/torch_stable.html -i https://mirrors.aliyun.com/pypi/simple && \
#-i https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple
COPY sources.list /etc/apt/
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 3B4FE6ACC0B21F32

RUN echo "==> Install..."  && \
    apt-get update && \
    apt-get install -y python3-pip libgomp1 gcc && \
    pip3 install -i https://mirrors.aliyun.com/pypi/simple --no-cache-dir --upgrade pip && \
    pip3 install -i https://mirrors.aliyun.com/pypi/simple gunicorn gevent && \
    pip3 install xgboost==2.0.3 && \ 
    pip3 install --no-cache-dir -r /src/requirements.txt -i http://mirrors.aliyun.com/pypi/simple --trusted-host mirrors.aliyun.com && \
    echo "==> Clean up..."  && \
    apt-get clean  && \
    rm -rf /var/lib/apt/lists/*  && \
    apt-get remove --auto-remove -y python3-pip


COPY . /src
RUN cd /src/ && \
    python3 setup.py && \
    mkdir /app/ && \
    cp -r build/* /app/ && \
    rm -r /src

WORKDIR /app
EXPOSE 8889
CMD ["gunicorn", "app:app", "--preload", "--timeout", "120", "--log-level", "info", "--access-logfile", "-", "--error-logfile", "-", "-c", "gunicorn.config.py"]
# CMD ["python", "main_job.py"]