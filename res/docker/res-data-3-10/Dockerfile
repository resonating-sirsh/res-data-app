#from a base of deps that are painful to build
FROM python:3.10

# Install system libraries
RUN apt-get update && apt-get install -y bash gcc librdkafka-dev python3-dev musl-dev ghostscript libvips-dev libcgal-dev 
RUN apt-get install ffmpeg libsm6 libxext6  -y --fix-missing

#scikitgeom
RUN git clone https://github.com/scikit-geometry/scikit-geometry  /scikit-geometry
WORKDIR /scikit-geometry
RUN pip install -e . -v
# add imagemagick - TODO: remove when we have good pyvips
RUN git clone https://github.com/ImageMagick/ImageMagick6.git /ImageMagick-6.9.12
WORKDIR /ImageMagick-6.9.12
RUN ./configure --with-quantum-depth=8 --disable-hdri
RUN make install
RUN ldconfig /usr/local/lib
#pyvips - whats a good one?
RUN pip install pyvips
COPY /res/docker/imagemagick/policy.xml /etc/ImageMagick-6/


ENV PYTHONPATH "${PYTHONPATH}:/app"
ENV PYTHONUNBUFFERED=0

#for testing the image
# RUN pip install jupyter
# COPY start_jupyter.sh /
# RUN chmod +x /start_jupyter.sh

##aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 286292902993.dkr.ecr.us-east-1.amazonaws.com
#docker build --pull --rm -f "Dockerfile" -t res-data310:latest --platform linux/amd64 ../../../
#docker tag res-data310:latest 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data310:latest & docker push 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data310:latest

#docker run -e RES_ENV='testing' --platform linux/amd64 --entrypoint pytest  -t res-data310 -m "not slow and not data and not service"
