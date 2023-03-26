FROM us-docker.pkg.dev/vertex-ai/training/tf-cpu.2-9:latest
WORKDIR /
COPY processor /processor
COPY service-account.json ./service-account.json
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt 
ENV URI_SERVICE_ENDPOINT=https://heart-predict-rdxftrkc7q-uc.a.run.app/predict
ENTRYPOINT ["python","-m","processor.task"]