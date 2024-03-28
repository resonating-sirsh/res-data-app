
#two options. first option is to use the full base and not PIP and the other is to use the "lighter" res-data (3.9 or 3.10) and pip
# for 3.10 we need to use a different requirements set (see the ones in the 3.10 lib)
#FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-libs310:latest
#FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data38:latest
FROM 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data310:latest
WORKDIR /app
#install the res library
COPY ./res/docker/res-data/requirements.txt /app/res/
######################################   PIP     ###################################################
#we can add the below or we can base of a lib docker image WIP
RUN pip install --no-cache-dir torch==2.0.0+cpu -f https://download.pytorch.org/whl/torch_stable.html
RUN pip install -U pip && pip install --no-cache-dir -r /app/res/requirements.txt 
# thing to make a pdf
RUN wget https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6.1-2/wkhtmltox_0.12.6.1-2.bullseye_amd64.deb
RUN dpkg -i wkhtmltox_0.12.6.1-2.bullseye_amd64.deb || true
RUN apt-get -f -y install
RUN pip install pdfkit
#####################################################################################################

COPY ./res /app/res/
COPY ./res_tests /app/res_tests/
COPY ./res-schemas /app/res-schemas/
COPY ./resources /app/resources/
ADD ./res-schemas/pydantic/ /app/schemas/pydantic/
#we use the ini file to configure pytest
COPY /res/docker/pytest.ini /app
# imagemagick configs ...
COPY /res/docker/imagemagick/policy.xml /etc/ImageMagick-6/

ENV PYTHONPATH "${PYTHONPATH}:/app"
ENV PYTHONUNBUFFERED=0

##aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 286292902993.dkr.ecr.us-east-1.amazonaws.com
#  docker build -t 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:latest --platform linux/amd64  \
#      --cache-to mode=max,image-manifest=true,oci-mediatypes=true,type=registry,ref=286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:cache \
#      --cache-from type=registry,ref=286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:cache ../../../




#docker build --pull --rm -f "Dockerfile" -t res-data:latest --platform linux/amd64 ../../../
#docker tag res-data:latest 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data:latest & docker push 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data:latest
#docker run -e RES_ENV='testing' --platform linux/amd64 --entrypoint pytest  -t res-data -m "not slow and not data and not service"

#docker tag argoproj/argocd:v2.4.6 286292902993.dkr.ecr.us-east-1.amazonaws.com/argo-cd:v2.4.6 & docker push 286292902993.dkr.ecr.us-east-1.amazonaws.com/argo-cd:v2.4.6