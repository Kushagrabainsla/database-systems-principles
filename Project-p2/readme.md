You need to install the GCC docker image for this assignment.  First install the Docker Desktop (Mac, Windows, or Linux) onto your laptop.  Then from a Terminal or Command Windows:  Do the following commands:

- Set the environment for this session; execute this export command ONLY if you have an M1 or M2 chip on your Mac

export DOCKER_DEFAULT_PLATFORM=linux/amd64

- Get the gcc image

docker pull gcc

- Create container with name: mygcc
- Using image: gcc

- Create container with name: mygcc
- Using image: gcc

docker run -itd --name mygcc --privileged=true -p 60000:60000 gcc

- Enter linux container shell

docker exec -ti mygcc bash -c "su"

- Sanity check 

gcc --version

- Will output: “gdb: can’t find command”

gdb --version

apt-get update

apt-get install gdb
- Answer yes to continue

gdb --version
- Sanity check

docker cp db.cpp container_id:/db.cpp

docker cp db.h container_id:/db.h
- Copy cpp file from host to docker container

gcc -g -o db db.cpp
- “-g” tag will set debug
- “-o” specifies output name