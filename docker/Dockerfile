FROM debian:trixie-slim AS base

# Update repositories
RUN apt-get update 

# Install dependencies
RUN apt-get install -y openssh-server
RUN apt-get install -y git
RUN apt-get install -y make
RUN apt-get -y install gcc 
RUN apt-get -y install vim --fix-missing
RUN apt-get -y install python3.12 python3-pip
RUN apt-get install -y sudo

# Python stuff
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.12 1
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1

# Create a user matching the builder's username
ARG USERNAME=parsl
ARG condaenv=env

RUN useradd -ms /bin/bash $USERNAME && echo "$USERNAME:redhat" | chpasswd
RUN adduser $USERNAME sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Configure SSH server
# Set no password for user login
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
EXPOSE 22

COPY ./Makefile /usr/local/bin
COPY ./cpu-benchmark.cpp /usr/local/bin
RUN cd /usr/local/bin && make && cd ~   

RUN python3 -m pip install --break-system-packages pandas pathos

# Switch to new user
USER $USERNAME
WORKDIR /home/$USERNAME

# SSH setup
RUN mkdir -p /home/$USERNAME/.ssh
COPY --chown=$USERNAME:$USERNAME ./id_rsa.pub /home/$USERNAME/.ssh/authorized_keys
RUN chmod 700 /home/$USERNAME/.ssh && \
chmod 600 /home/$USERNAME/.ssh/authorized_keys && \
chown -R $USERNAME:$USERNAME /home/$USERNAME/.ssh


COPY --chown=$USERNAME:$USERNAME ./entry.sh /bin
RUN chmod +x /bin/entry.sh

FROM base AS builder

ARG CACHEBUST=1

COPY ./wfbench /usr/local/bin

# Install parsl
RUN git clone --depth 1 --branch scheduling_using_simulation https://github.com/jeff-yc-wong/parsl.git
RUN sudo python3 -m pip install --break-system-packages parsl/

# Install WfCommons
# RUN git clone --depth 1 --branch feature/wfbench_updates https://github.com/wfcommons/WfCommons.git
# RUN pip install WfCommons/



ENTRYPOINT ["/bin/entry.sh"]
