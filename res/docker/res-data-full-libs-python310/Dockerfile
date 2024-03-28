#there are two things we can do. we can use the base 3.10 with the big libs and then do all the pip from reqs
#or we can do it all at once and build this docker image when we get new reqs either manually and pushing the base or as a git action if requirements change
#this can be very slow and big so we are trying to optimize for size and speed
#then we have a choice about which image to base our normal lib on
#for example we could have a res-data frequent build that just bases on this one with a code copy.
#but we would like to have a docker image < 1.5G
#from a base of deps that are painful to build   && rm -rf /var/lib/apt/lists/*
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

# add standard libs
COPY ./res/docker/res-data-full-libs-python310/requirements.txt /app/res/
#install torch first
RUN pip install --no-cache-dir torch==2.0.0+cpu -f https://download.pytorch.org/whl/torch_stable.html
#install any changes to requirements ^ does this order matter, im trying to just isolate changes
RUN pip install -U pip && pip install --no-cache-dir -r /app/res/requirements.txt 

ENV PYTHONPATH "${PYTHONPATH}:/app"
ENV PYTHONUNBUFFERED=0

##aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 286292902993.dkr.ecr.us-east-1.amazonaws.com
#docker build --pull --rm -f "Dockerfile" -t res-data-libs310:latest --platform linux/amd64 ../../../ 
#docker tag res-data-libs310:latest 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-libs310:latest & docker push 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-libs310:latest
#docker run -e RES_ENV='testing' --platform linux/amd64 --entrypoint pytest  -t res-data-libs310 -m "not slow and not data and not service"
#docker history res-data-libs310