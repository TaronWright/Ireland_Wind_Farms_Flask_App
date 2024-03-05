FROM python:3.12


# set the working directory
WORKDIR /app

# install dependencies
COPY ./requirements.txt /app
RUN pip install --no-cache-dir --upgrade -r requirements.txt

#copy the scripts to the folder
COPY . /app

#start the server
CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0", "--port", "80"]
