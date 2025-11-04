FROM python:3.11.14-slim

RUN pip install --upgrade pip setuptools
RUN apt-get update

RUN apt-get install -y expat
WORKDIR /app

COPY requirements.txt .
COPY . .

ENV CREODIAS_S3_ACCESS_KEY=NONE
ENV CREODIAS_S3_SECRET_KEY=NONE
ENV CREODIAS_REGION=WAW4-1
ENV CREODIAS_ENDPOINT=https://s3.waw4-1.cloudferro.com
ENV CREODIAS_S3_BUCKET_PRODUCT_OUTPUT=noa

RUN chmod +x /app/noachdm/cli.py
RUN pip install -r requirements.txt --no-cache-dir

ENTRYPOINT ["python", "noachdm/cli.py"]

LABEL org.opencontainers.image.source=https://github.com/noa-beyond/eoProcessors/
LABEL org.opencontainers.image.description="National Observatory of Athens - Change Detection Mapping Processor"
LABEL org.opencontainers.image.licenses="GNU Affero General Public License v3.0"