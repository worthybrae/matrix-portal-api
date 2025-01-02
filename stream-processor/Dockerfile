FROM public.ecr.aws/lambda/python:3.11
WORKDIR /var/task

# Install system dependencies
RUN yum update -y && yum install -y \
    tar \
    xz \
    wget \
    gcc \
    gcc-c++ \
    make \
    cmake \
    gtk3-devel \
    git

# Install FFmpeg
RUN mkdir -p /opt/bin && \
    cd /opt && \
    wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz && \
    tar xf ffmpeg-release-amd64-static.tar.xz && \
    mv ffmpeg-*-amd64-static/ffmpeg /opt/bin/ && \
    mv ffmpeg-*-amd64-static/ffprobe /opt/bin/ && \
    chmod +x /opt/bin/ffmpeg /opt/bin/ffprobe && \
    rm -rf ffmpeg-*-amd64-static*

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY lambda_function.py .
CMD ["lambda_function.handler"]
