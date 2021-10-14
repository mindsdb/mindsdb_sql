FROM python:3.8-slim
# RUN apt-get update && apt-get install -y bash \
#                                     gcc \
#                                     python3-dev \
#                                     musl-dev \
#                                     libxml2-dev \
#                                     libxslt-dev \
#                                     libyaml-dev libpython3-dev \
#                                     libpq-dev \
#                                     ffmpeg \
#                                     curl \
#                                     git \
#                    && pip install --no-cache uwsgi

WORKDIR /app/
COPY . /app
ENV PYTHONPATH=/app/

RUN pip install --no-cache -r /app/requirements.txt
RUN pip install --no-cache -e .

