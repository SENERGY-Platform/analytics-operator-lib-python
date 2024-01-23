FROM python:3.11
WORKDIR /usr/src/app
# you should generate the git_commit file with: git log -1 --pretty=format:"commit=%H%ndate=%cd%n" > git_commit
COPY git_commit example/git_commit
ADD requirements.txt .
RUN apt-get update && apt-get install -y git librdkafka-dev && python3 -m pip install --no-cache-dir -r requirements.txt && apt-get purge -y git && apt-get auto-remove -y && apt-get clean 
COPY . .
CMD [ "python3", "-u", "main.py"]
